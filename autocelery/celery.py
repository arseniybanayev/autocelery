import celery

celery_app = celery.Celery('grid')
celery_app.config_from_object('grid.celeryconfig')