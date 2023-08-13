from kombu.utils.imports import symbol_by_name


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See Also:
        :func:`symbol_by_name`.
    """
    return symbol_by_name(name)(*args, **kwargs)


def reprkwargs(kwargs, sep=', ', fmt='{0}={1}'):
    return sep.join(fmt.format(k, v) for k, v in kwargs.items())


def reprcall(name, args=(), kwargs=None, sep=', '):
    kwargs = {} if not kwargs else kwargs
    return '{}({}{}{})'.format(
        name, sep.join(args or ()),
        (args and kwargs) and sep or '',
        reprkwargs(kwargs, sep),
    )


def truncate(s, maxlen=128, suffix="..."):
    # type: (str, int, str) -> str
    """Truncate text to a maximum number of characters."""
    if maxlen and len(s) >= maxlen:
        return s[:maxlen].rsplit(" ", 1)[0] + suffix
    return s
