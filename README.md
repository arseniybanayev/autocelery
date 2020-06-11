# autocelery

`autocelery` adds a special Celery task to your Celery application that copies your current local code and environment to your workers, allowing you to declare and run Celery tasks using your local code without restarting your workers or reloading their code or modules.

## Motivation

### Reloading in basic Celery...

... is not supported. According to [the Celery docs](https://docs.celeryproject.org/en/stable/userguide/tasks.html#how-it-works):
>When tasks are sent, no actual function code is sent with it, just the name of the task to execute. When the worker then receives the message it can look up the name in its task registry to find the execution code.
>
>This means that your workers should always be updated with the same software as the client. This is a drawback, but the alternative is a technical challenge that's yet to be solved.

This traditionally implies that you must declare your Celery tasks, deploy the code to your worker cluster, and start the cluster before you can run those tasks on that cluster.

Historically, Celery had an experimental `--autoreload` flag, but it was deprecated around v3.1 and [officially removed in v4.0](https://docs.celeryproject.org/en/stable/history/whatsnew-4.0.html#features-removed-for-lack-of-funding).

### Why not use `watchdog` and `watchmedo`?

Some developers use `watchdog` and `watchmedo` to watch the local code directories for changes and restart Celery workers when something is changed, similar to `gunicorn --reload` or web frameworks like `Flask` or `django` in development.

However, you would not deploy this solution to production: it's a supervisor/orchestration solution and it would add nesting and complexity to your existing production service orchestration. Moreover, you would not want your production services to depend on local code (what does "local" mean in production, anyway?).

But you might want a version of auto-reloading in production if
1. You have beefy machines running your prod worker cluster
2. You are iterating on code that needs a beefy cluster to evaluate

## Getting started
...