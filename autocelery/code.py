import os
import tarfile
import tempfile
import hashlib
import sys

from redis_client import redis, locks

def cache_local_python_code(job_id: str):
    """
    Archives and compresses all local python source code, hashes the
    bytes and saves the bytes to Redis under key 'grid:tar:(job_id)'.
    """

    # Archive and compress local python source code to a byte array
    tar_filter = lambda f: None if '__pycache__' in f.name else f
    with tempfile.TemporaryFile(suffix='.tar.gz') as f:
        with tarfile.open(fileobj=f, mode='w:gz') as tar:
            # TODO perhaps put all python code into one super-directory. This list reoccurs
            for directory in [
                'chat_relay',
                'chats',
                'common',
                'drift',
                'es',
                'homes',
                'mail_router',
                'mail_sender',
                'media',
                'pushy',
                'users',
                'website'
            ]: tar.add(directory, filter=tar_filter)

        # Save the archived and compressed bytes to Redis
        f.flush()
        f.seek(0)
        tar_bytes = f.read()
        saved = redis.setnx(f'grid:tar:{job_id}', tar_bytes)
        if saved:
            print(f"Saved tar file to redis under 'grid:tar:{job_id}'")
        else:
            print(f"Skipping redis save, tar file already exists at 'grid:tar:{job_id}'")


def load_code_from_redis(job_id: str, celery_worker_hostname: str):
    """
    Runs on worker process prior to creating subprocess for task
    execution.

    Loads tar bytes from Redis under key 'grid:tar:(job_id)', saves those
    bytes to a tar file, then uncompresses and extracts the contents to
    a temporary directory. Returns the path to that temporary directory.
    """

    # We might not need to go to Redis for the tar bytes
    path_to_code = os.path.join(tempfile.gettempdir(), f'dd_{job_id}')
    with locks.Lock(celery_worker_hostname, wait_ms=1000):
        if os.path.isdir(path_to_code):
            print(f'App code for job {job_id} already exists at {path_to_code}')
            return path_to_code
        
        # Get tar bytes from Redis by hash
        tar_bytes = redis.get(f'grid:tar:{job_id}')
        with tempfile.TemporaryFile(suffix='.tar.gz') as f:
            f.write(tar_bytes)
            f.flush()
            f.seek(0)

            # Uncompress and extract the code directory
            with tarfile.open(fileobj=f, mode='r:gz') as tar:
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tar, path_to_code)
    
    print(f'Downloaded app code for job {job_id} to {path_to_code}')
    return path_to_code