from contextlib import contextmanager

from distributed import rejoin, secede


@contextmanager
def with_secede():
    try:
        secede()
    except KeyError:
        yield
    else:
        yield
        rejoin()
