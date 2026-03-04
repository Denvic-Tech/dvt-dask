from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import ClassVar

__all__ = ["Callback", "add_callbacks"]


class Callback:
    """Base class for using the callback mechanism

    Create a callback with functions of the following signatures:

    >>> def start(dsk):
    ...     pass
    >>> def start_state(dsk, state):
    ...     pass
    >>> def pretask(key, dsk, state):
    ...     pass
    >>> def posttask(key, result, dsk, state, worker_id):
    ...     pass
    >>> def finish(dsk, state, failed):
    ...     pass
    >>> def operation_start(op_meta, dsk, state):
    ...     pass
    >>> def partition_event(op_meta, dsk, state):
    ...     pass
    >>> def operation_end(op_meta, dsk, state):
    ...     pass
    >>> def operation_error(op_meta, exc, dsk, state):
    ...     pass

    You may then construct a callback object with any number of them

    >>> cb = Callback(pretask=pretask, finish=finish)

    And use it either as a context manager over a compute/get call

    >>> with cb:            # doctest: +SKIP
    ...     x.compute()

    Or globally with the ``register`` method

    >>> cb.register()
    >>> cb.unregister()

    Alternatively subclass the ``Callback`` class with your own methods.

    >>> class PrintKeys(Callback):
    ...     def _pretask(self, key, dask, state):
    ...         print("Computing: {0}!".format(repr(key)))

    >>> with PrintKeys():   # doctest: +SKIP
    ...     x.compute()
    """

    active: ClassVar[set[tuple[Callable | None, ...]]] = set()

    def __init__(
        self,
        start=None,
        start_state=None,
        pretask=None,
        posttask=None,
        finish=None,
        operation_start=None,
        partition_event=None,
        operation_end=None,
        operation_error=None,
    ):
        if start:
            self._start = start
        if start_state:
            self._start_state = start_state
        if pretask:
            self._pretask = pretask
        if posttask:
            self._posttask = posttask
        if finish:
            self._finish = finish
        if operation_start:
            self._operation_start = operation_start
        if partition_event:
            self._partition_event = partition_event
        if operation_end:
            self._operation_end = operation_end
        if operation_error:
            self._operation_error = operation_error

    @property
    def _callback(self) -> tuple[Callable | None, ...]:
        fields = [
            "_start",
            "_start_state",
            "_pretask",
            "_posttask",
            "_finish",
            "_operation_start",
            "_partition_event",
            "_operation_end",
            "_operation_error",
        ]
        return tuple(getattr(self, i, None) for i in fields)

    def __enter__(self):
        self._cm = add_callbacks(self)
        self._cm.__enter__()
        return self

    def __exit__(self, *args):
        self._cm.__exit__(*args)

    def register(self) -> None:
        Callback.active.add(self._callback)

    def unregister(self) -> None:
        Callback.active.remove(self._callback)


def unpack_callbacks(cbs):
    """Take an iterable of callbacks, return a list of each callback."""
    if cbs:
        return [[i for i in f if i] for f in zip(*cbs)]
    else:
        return [(), (), (), (), (), (), (), (), ()]


@contextmanager
def local_callbacks(callbacks=None):
    """Allows callbacks to work with nested schedulers.

    Callbacks will only be used by the first started scheduler they encounter.
    This means that only the outermost scheduler will use global callbacks."""
    global_callbacks = callbacks is None
    if global_callbacks:
        callbacks, Callback.active = Callback.active, set()
    try:
        yield callbacks or ()
    finally:
        if global_callbacks:
            Callback.active = callbacks


def normalize_callback(cb):
    """Normalizes a callback to a tuple"""
    if isinstance(cb, Callback):
        return cb._callback
    elif isinstance(cb, tuple):
        if len(cb) == 5:
            return cb + (None, None, None, None)
        if len(cb) == 8:
            return cb + (None,)
        if len(cb) == 9:
            return cb
        raise ValueError(
            f"Expected callback tuple length 5, 8, or 9, got {len(cb)}."
        )
    else:
        raise TypeError("Callbacks must be either `Callback` or `tuple`")


class add_callbacks:
    """Context manager for callbacks.

    Takes several callbacks and applies them only in the enclosed context.
    Callbacks can either be represented as a ``Callback`` object, or as a tuple
    of length 5 (legacy), 8 (extended without operation-error hook),
    or 9 (extended).

    Examples
    --------
    >>> def pretask(key, dsk, state):
    ...     print("Now running {0}").format(key)
    >>> callbacks = (None, None, pretask, None, None)
    >>> with add_callbacks(callbacks):    # doctest: +SKIP
    ...     res.compute()
    """

    def __init__(self, *callbacks):
        self.callbacks = [normalize_callback(c) for c in callbacks]
        Callback.active.update(self.callbacks)

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        for c in self.callbacks:
            Callback.active.discard(c)
