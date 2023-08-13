from kombu.pools import producers

from celery.queues import task_exchange
from celery.tasks import hello_task

priority_to_routing_key = {
    "high": "hipri",
    "mid": "midpri",
    "low": "lopri",
}


def send_as_task(connection, fun, args=(), kwargs={}, priority="mid"):
    payload = {"fun": fun, "args": args, "kwargs": kwargs}
    routing_key = priority_to_routing_key[priority]

    with producers[connection].acquire(block=True) as producer:
        producer.publish(
            payload,
            serializer="pickle",
            compression="bzip2",
            exchange=task_exchange,
            declare=[task_exchange],
            routing_key=routing_key,
        )


if __name__ == "__main__":
    from kombu import Connection

    connection = Connection("redis://")
    send_as_task(
        connection, fun=hello_task, args=("Kombu",), kwargs={}, priority="high"
    )
