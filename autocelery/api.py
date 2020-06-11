import os
import time
import itertools

from grid import task


# General grid TODO
    # TODO introduce affinity for code reuse so workers don't have to re-download from redis
    #   and re-uncompress and re-extract, and for Flask app_context so that Flask app instances
    #   don't have to be created over and over and DB connections can persist
    # TODO periodically delete code stored in Redis and other Redis artifacts from grid jobs
    # TODO allow scaling the # and size of workers up and down from client commands


class GridJob:
    """
    Parameters
    ----------
    func : function
        Pickle-able function defined by you in the interpreter or imported from a module.
    *iterables : list of lists
        Positional argument lists. The first task is 'func' applied to the first element of each list,
        the second task is 'func' applied to the second element of each list, and so on. Thus, the lists
        must have the same length (and that length will be equal to the number of resulting tasks), and
        the number of lists must match the number of positional arguments in 'func'.
    extra_packages : list, optional
        A list of pip package names (strings) that should be installed for the tasks to run properly.
        Default is [].
    capture_environment : list, optional
        A list of environment variable names (strings) that should be copied from the calling environment
        for the tasks to run properly. Default is [].
    environment : dict, optional
        A dictionary of environment variables to set for the tasks to run properly. These explicit
        environment variables have priority over those copied by 'capture_environment'. Default is {}.
    task_timeout : int, optional
        Task executions will time out after this many seconds. Default is 60.
    """
    def __init__(self, func, extra_packages=[], capture_environment=[], environment={}, task_timeout=None):
        self.settings = task.client_prepare_task_settings(func, extra_packages, capture_environment, environment, task_timeout)
    
    def add(self, *args, **kwargs):
        kwargs['__settings'] = self.settings
        return task.run.delay(*args, **kwargs)

def map(func, *iterables, extra_packages=[], capture_environment=[], environment={}, task_timeout=None, **kwargs):
    if len(iterables) == 0:
        raise ValueError("*iterables must have at least one element")
    if any([len(ible) != len(iterables[0]) for ible in iterables]):
        raise ValueError("Every element of *iterables must have the same length")
    
    job = GridJob(func, extra_packages, capture_environment, environment, task_timeout)
    for i in range(len(iterables[0])):
        args = [ible[i] for ible in iterables]
        yield job.add(*args, **kwargs)

def wait(tasks):
    """
    Blocks until 'tasks' are all either finished or errored. Prints aggregate task statuses every 3 seconds.
    """
    while True:
        error = 0
        finished = 0
        for t in tasks:
            if t.done():
                if t.exception() is not None: error += 1
                else: finished += 1
        pending = len(tasks) - finished - error
        print(f'grid: finished: {finished}, error: {error}, pending: {pending}')
        if pending == 0:
            break
        time.sleep(3)
    print('Summary:')
    print(f' Successfully completed {len([t for t in tasks if t.done() and t.exception() is None])} tasks')
    num_errors = len([t for t in tasks if t.done() and t.exception() is not None])
    if num_errors > 0:
        print(f' Errors in {len([t for t in tasks if t.done() and t.exception() is not None])} tasks')
        print(f' 3 random errors:')
        i = 0
        for t in tasks:
            if i >= 3: break
            if t.done() and t.exception() is not None:
                i += 1
                print(f' {t.exception()}')