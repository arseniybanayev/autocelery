import traceback
import sys
import cloudpickle
import importlib
import tempfile
import subprocess
import uuid
import os
from tblib import pickling_support

import pexpect

from grid import code
from grid.celery import celery_app

@pickling_support.install
class SerializableException(Exception):
    """
    Supports serializing Exception objects with traceback,
    so they can be re-raised identically after deserializing.
    Decorated with tblib.pickling_support.install:
    https://github.com/ionelmc/python-tblib#pickling-tracebacks
    """

    def __init__(self, exception: Exception):
        self.exception = exception
        self.traceback: sys.types.TracebackType
        _, _, self.traceback = sys.exc_info()
    
    def reraise(self):
        raise self.exception.with_traceback(self.traceback)

def client_prepare_task_settings(func, extra_packages, capture_environment, environment, task_timeout):
    """
    Generates task settings to be serialized to the worker process and consumed there by run()
    and by the task subprocess it spawns. Packages and uploads user code, captures user env
    vars, serializes the user func, and prepares any additional pre-task setup. Returns the settings
    """
    settings = {}

    # Choose a job ID, used for finding code in redis
    job_id = str(uuid.uuid4())
    print(f'Job ID will be {job_id}')
    settings['job_id'] = job_id

    # Upload local code for use in the task
    code.cache_local_python_code(job_id)

    # Save sys.path for (selective) repopulation on the task side
    settings['extra_sys_paths'] = [p for p in sys.path if p.startswith('/dd')]

    # Prepare environment variables
    capture_environment = capture_environment or []
    environment = environment or {}
    for v in capture_environment:
        if v in environment: continue
        environment[v] = os.environ.get(v)
    settings['environment'] = environment

    # Prepare the user func. Serialize user func and module name, and
    # serialize function body if it is defined in the interpreter/__main__
    if func.__module__ == '__main__':
        settings['f'] = func
    else:
        settings['func_name'] = func.__name__
        settings['func_module'] = func.__module__
    
    # Packages to install before running the user func
    settings['extra_packages'] = extra_packages or []

    # How long, in secs, the task has to live before being killed by a TIMEOUT
    settings['task_timeout'] = task_timeout or 60

    return settings

@celery_app.task(bind=True)
def run(self, *args, **kwargs):
    """
    Serializable func that is called remotely on the worker process. kwargs contain user kwargs
    and task settings (under key '__settings'). Installs extra pip packages, then creates or
    reuses a task subprocess for running the user func. Reraises exceptions from that user func,
    otherwise returns its result.
    """
    settings: dict = kwargs.pop('__settings')

    # Install packages worker-wide using pip in a subprocess
    extra_packages = settings.pop('extra_packages', [])
    if len(extra_packages) > 0:
        package_list = ' '.join(extra_packages)
        print(f'Installing extra packages with pip: {package_list}')
        subprocess.run([sys.executable, '-u', '-m', 'pip', 'install', package_list], check=True)
    else:
        print('No extra packages to install')
    
    # Load user code
    #   (We do this here and not inside the task subprocess, because we need
    #   to lock the part of the filesystem that is being modified or else
    #   multiple worker processes in the same container will stomp on each other,
    #   and the only way to identify the worker is in the worker process)
    job_id = settings.pop('job_id')
    celery_worker_hostname = self.request.hostname
    print(f'Loading code for job {job_id} on {celery_worker_hostname}')
    path_to_code = code.load_code_from_redis(job_id, celery_worker_hostname)
    
    # Extract task timeout from settings
    task_timeout = settings.pop('task_timeout')
    
    # Serialize the rest of the settings
    path_to_settings = os.path.join(tempfile.gettempdir(), f'{uuid.uuid4()}.pkl')
    with open(path_to_settings, 'wb') as settings_file:
        cloudpickle.dump(settings, settings_file)
    
    # Run user func inside the task subprocess
    sp = worker_get_subprocess(job_id, task_timeout, path_to_code, path_to_settings)
    return worker_run_task_in_subprocess(sp, *args, **kwargs)

task_subprocess: pexpect.spawn = None
current_job_id = None

def worker_get_subprocess(job_id, task_timeout, path_to_code, path_to_settings) -> pexpect.spawn:
    global task_subprocess
    global current_job_id
    
    last_job_id = current_job_id
    if task_subprocess is not None:
        if last_job_id != job_id or task_subprocess.eof():
            # Finish the last subprocess if it wasn't for the same job ID
            print(f'Closing last subprocess, belonging to job {last_job_id}')
            task_subprocess.close(True)
            task_subprocess = None
    
    # Start a new subprocess for this job ID
    if task_subprocess is None:
        print(f'Starting new subprocess for job {job_id}')
        task_subprocess = pexpect.spawn(
            sys.executable,
            ['-u', '-m', 'grid.task', path_to_code, path_to_settings],
            timeout=task_timeout,
            maxread=1,
            encoding='utf-8')
        task_subprocess.logfile = sys.stdout
        current_job_id = job_id
    
    return task_subprocess

def worker_run_task_in_subprocess(sp: pexpect.spawn, *args, **kwargs):
    # TODO check the buffer size rules for stdin and stdout and directly
    # stream bytes instead of saving them to the file system. See 'encoding='
    # argument in pexpect.spawn constructor
    
    # Serialize args and kwargs to a file
    path_to_args = os.path.join(tempfile.gettempdir(), f'{uuid.uuid4()}.pkl')
    with open(path_to_args, 'wb') as args_file:
        cloudpickle.dump([args, kwargs], args_file)
    
    # Prepare result and error files
    path_to_result = os.path.join(tempfile.gettempdir(), f'{uuid.uuid4()}.pkl')
    path_to_error = os.path.join(tempfile.gettempdir(), f'{uuid.uuid4()}.pkl')

    # Communicate to the subprocess
    sp.expect('Ready for next task.')  # Printed in the subprocess loop
    sp.sendline(path_to_args)
    sp.sendline(path_to_result)
    sp.sendline(path_to_error)

    # Wait for the subprocess to complete
    sp.expect('Task complete.')  # Printed in the subprocess loop

    # Check for an exception to be reraised
    error: SerializableException = None
    try:
        with open(path_to_error, 'rb') as error_file:
            error: SerializableException = cloudpickle.load(error_file)
    except FileNotFoundError:
        pass
    
    # Check for a result to be returned
    result = None
    try:
        with open(path_to_result, 'rb') as result_file:
            result = cloudpickle.load(result_file)
    except FileNotFoundError as e:
        pass

    # Remove the args, result and error files
    try: os.remove(path_to_args)
    except (FileNotFoundError, NameError): pass
    try: os.remove(path_to_result)
    except (FileNotFoundError, NameError): pass
    try: os.remove(path_to_error)
    except (FileNotFoundError, NameError): pass

    # Finish up (reraise error or return result)
    if error is not None:
        error.reraise()
    return result

def subprocess_initialize(path_to_code, path_to_settings):
    """
    Called when the task subprocess starts up. Prepares sys.path,
    sets environment variables, unpickles/imports/recreates the user
    func. Returns the user func.
    """
    
    # Unpickle the settings that we pickled in the worker process
    with open(path_to_settings, 'rb') as settings_file:
        settings = cloudpickle.load(settings_file)
    
    # Make sure this task subprocess knows where to look for modules
    extra_sys_paths = [p for p in settings.pop('extra_sys_paths') if p not in sys.path]
    sys.path = [p.replace('/dd', path_to_code) for p in extra_sys_paths + sys.path]
    if path_to_code not in sys.path:
        sys.path = [path_to_code] + sys.path
    print(f'sys.path is now: {sys.path}')
    
    # Set environment variables
    environment = settings.pop('environment')
    for k in environment:
        os.environ[k] = environment[k]

    # Find the func in the module, or recreate it from __main__
    user_func = settings.pop('f', None)  # For functions defined in __main__
    if user_func is None:
        # For other functions that come from a module in the loaded user code
        module = importlib.import_module(settings.pop('func_module'))
        user_func = getattr(module, settings.pop('func_name'))
    
    return user_func

def subprocess_loop(user_func):
    try:
        while True:
            print('Ready for next task.')  # Expected by pexpect
            path_to_args = sys.stdin.readline().replace(os.linesep, '')
            path_to_result = sys.stdin.readline().replace(os.linesep, '')
            path_to_error = sys.stdin.readline().replace(os.linesep, '')
            
            # Deserialize args and kwargs and run the user func
            with open(path_to_args, 'rb') as args_file:
                [args, kwargs] = cloudpickle.load(args_file)
            try:
                result = user_func(*args, **kwargs)

                # Serialize the result
                with open(path_to_result, 'wb') as result_file:
                    cloudpickle.dump(result, result_file)
            except Exception as ex:
                # Serialize the exception
                print(ex)
                serializable_ex = SerializableException(ex)
                with open(path_to_error, 'wb') as error_file:
                    cloudpickle.dump(serializable_ex, error_file)

            # Tell the worker process that we're done
            print('Task complete.')  # Expected by pexpect
    except:
        print(traceback.format_exc())
        print('Ending subprocess')

if __name__ == '__main__':
    # Initialize the task subprocess (incl. sys.path, env vars, user func)
    path_to_code = sys.argv[1]
    path_to_settings = sys.argv[2]
    user_func = subprocess_initialize(path_to_code, path_to_settings)

    # Run tasks in a loop when pexpect in the worker process tells us to
    subprocess_loop(user_func)
    print('Subprocess ended for some reason')