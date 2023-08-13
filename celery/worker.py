import socket
import importlib
import kombu

from kombu.asynchronous import Hub
from kombu.utils import reprcall

from celery.utils import instantiate



def prt_ret(ret):
    print("return is %s" % ret)

#: States
RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

class Worker:

    hub = None
    state = None

    def __init__(self, name, app_path, pool_type, concurrency, logger):
        self.name = name or "celery-lite-worker"
        self.min_concurrency = concurrency or 10
        self.pool_type = pool_type or "thread"
        self.logger = logger

        self.exchange_name = "tasks"
        self.routing_key = "celery-lite"

        self._connection = None
        self._taskpool = None

        self.app = self._find_app(app_path)

        if pool_type in ("thread",):
            self.hub = Hub()

    def _find_app(self, app_path):
        slice = app_path.split(".")
        module_path = ".".join(slice[:-1])
        module = importlib.import_module(module_path)
        return module.app

    def get_connection(self):
        self._connection = kombu.Connection(
            "redis://", transport_options={"visibility_timeout": 3000}
        )
        return self._connection

    def on_message(self, message, callback=prt_ret):
        body = message.decode()

        name = body["name"]
        args = body["args"]
        kwargs = body["kwargs"]

        func = self.app.tasks[name]
        self.on_task_received(func, args, kwargs, callback=callback)

        message.ack()

    def on_task_received(self, func, args, kwargs, callback):
        self.logger.info("Got task: %s", reprcall(func.__name__, args, kwargs))

        try:
            self._taskpool.apply_async(func, args, kwargs, callback=callback)
        except Exception as exc:
            self.logger.error("task raised exception: %r", exc)

    def start(self):
        self.logger.info("start worker")
        self.state = RUN
        self.start_task_pool()
        self.loop()
        self.logger.info("worker is stopped")

    def start_task_pool(self):
        self.logger.info("start task pool")
        pool_cls = "celery.concurrency.{}:TaskPool".format(self.pool_type)
        self._taskpool = instantiate(pool_cls, self.min_concurrency)
        self._taskpool.start()

    def heartbeat(self):
        self.logger.info("worker:[%s] is alive" % self.name)

    def consumer_queue(self):
        task_exchange = kombu.Exchange("tasks", type="direct")
        q = kombu.Queue("celery-lite", task_exchange, routing_key=self.routing_key),

        return q

    def stop(self):
        if self.hub:
            self.hub.close()

        self.state = TERMINATE
        self.logger.info("worker is terninated")

    def loop(self):
        connection = self.get_connection()
        connection.connect()
        task_exchange = kombu.Exchange("tasks", type="direct")
        q = kombu.Queue("celery-lite", task_exchange, routing_key="celery-lite"),

        self.consumer = consumer = connection.Consumer(
            queues=q,
            accept=["pickle", "json"],
            on_message=self.on_message,
        )
        consumer.consume()

        try:
            if self.hub:
                connection.transport.register_with_event_loop(
                    connection.connection, self.hub
                )

                self.hub.timer.call_repeatedly(10, self.heartbeat)
                self.hub.run_forever()
            else:
                while self.state == RUN:
                    try:
                        connection.drain_events(timeout=2.0)
                    except socket.timeout:
                        pass
        except KeyboardInterrupt:
            self.consumer.close()
            self.hub.close()
        except SystemExit as exc:
            self.stop(exitcode=exc.code)

        self.logger.info("loop ends")
