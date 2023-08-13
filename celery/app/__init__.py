import kombu

from logging.config import DEFAULT_LOGGING_CONFIG_PORT
from kombu import Connection
from kombu.pools import producers
from kombu.utils.imports import symbol_by_name


DEFAULT_CONFIG = {"connection_url": "redis://"}

def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See Also:
        :func:`symbol_by_name`.
    """
    return symbol_by_name(name)(*args, **kwargs)

class App:
    def __init__(self, config=DEFAULT_CONFIG):
        self.config = config
        self.task_module = "tasks.tasks"
        self.tasks = {}

    def connection(self):
        connection = Connection(self.config["connection_url"])
        return connection

    def task(self, func):
        self.tasks[func.__module__ + ":" + func.__name__] = func
        return Task(self, func)


class Task:

    def __init__(self, app, func):
        self.app = app
        self.name = ":".join([func.__module__, func.__name__])

    def delay(self, *args, **kwargs):
        payload = {"name": self.name, "args": args, "kwargs": kwargs}
        routing_key = "celery-lite"
        connection = Connection("redis://")
        task_exchange = kombu.Exchange("tasks", type="direct")

        with producers[connection].acquire(block=True) as producer:
            producer.publish(
                payload,
                serializer="pickle",
                compression="bzip2",
                exchange=task_exchange,
                declare=[task_exchange],
                routing_key=routing_key,
            )
