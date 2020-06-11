import os

# https://docs.celeryproject.org/en/stable/userguide/configuration.html

broker_url = os.environ['REDIS_URL']
result_backend = os.environ['REDIS_URL']

task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['pickle']

timezone = 'US/Eastern'

imports = ['grid.task']