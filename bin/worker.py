import click
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@click.command
@click.option("--name", default="celery-lite-worker", help="worker name")
@click.option("--app", default="app", help="app")
@click.option("--pool", default="thread", help="pool type: (thread)")
@click.option("--concurrency", default=4, help="concurrency number")
def worker(name, pool, concurrency, app):
    from celery.worker import Worker

    w = Worker(name=name, pool_type=pool, concurrency=concurrency, app_path=app, logger=logger)
    w.start()


if __name__ == "__main__":
    worker()
