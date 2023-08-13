import logging

from kombu.utils.encoding import safe_repr
from celery.utils import truncate

logger = logging.getLogger(__name__)



def apply_target(target, args=(), kwargs=None, callback=None, accept_callback=None):
    """Apply function within pool context."""
    kwargs = {} if not kwargs else kwargs
    # if accept_callback:
    #     accept_callback(pid or getpid(), monotonic())
    try:
        ret = target(*args, **kwargs)
    except Exception:
        raise
    else:
        callback(ret)


class BasePool:

    RUN = 0x1

    def __init__(self, limit=None, **kwrags):
        self.limit = limit
        self.logger = kwrags.get("logger", logger)

    def on_start(self):
        pass

    def did_start_ok(self):
        return True

    def flush(self):
        pass

    def on_stop(self):
        pass

    def register_with_event_loop(self, loop):
        pass

    def on_apply(self, *args, **kwargs):
        pass

    def on_terminate(self):
        pass

    def on_soft_timeout(self, job):
        pass

    def on_hard_timeout(self, job):
        pass

    def maintain_pool(self, *args, **kwargs):
        pass

    def terminate_job(self, pid, signal=None):
        raise NotImplementedError(f"{type(self)} does not implement kill_job")

    def restart(self):
        raise NotImplementedError(f"{type(self)} does not implement restart")

    def stop(self):
        self.on_stop()

    def terminate(self):
        self.on_terminate()

    def start(self):
        self._does_debug = logger.isEnabledFor(logging.DEBUG)
        self.on_start()
        self._state = self.RUN

    def close(self):
        self._state = self.CLOSE
        self.on_close()

    def on_close(self):
        pass

    def apply_async(self, target, args=None, kwargs=None, **options):
        """Equivalent of the :func:`apply` built-in function.

        Callbacks should optimally return as soon as possible since
        otherwise the thread which handles the result will get blocked.
        """
        kwargs = {} if not kwargs else kwargs
        args = [] if not args else args
        if self._does_debug:
            logger.debug(
                "TaskPool: Apply %s (args:%s kwargs:%s)",
                target,
                truncate(safe_repr(args), 1024),
                truncate(safe_repr(kwargs), 1024),
            )

        return self.on_apply(target, args, kwargs, **options)

    def _get_info(self):
        return {
            "max-concurrency": self.limit,
        }

    @property
    def info(self):
        return self._get_info()

    @property
    def active(self):
        return self._state == self.RUN

    @property
    def num_processes(self):
        return self.limit
