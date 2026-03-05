from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from queue import Queue
from threading import Condition, Lock, Thread
from typing import Any

from dask._task_spec import OperationMeta
from dask.callbacks import Callback
from dask.tokenize import _tokenize_deterministic

_OPERATION_CALLBACKS_ATTR = "__dvt_operation_callbacks_spec"
_RESERVED_METADATA_KEYS = {"ddf_meta", "operation_id", "partition_info", "exc"}
_COPY_MODES = {"deep", "shallow", "none"}
_PARTITION_DISPATCH_MODES = {"sync", "threaded"}
_DEFAULT_PARTITION_DISPATCH_WORKERS = 1
_DEFAULT_PARTITION_DISPATCH_QUEUE_SIZE = 256


def _get_operation_callbacks_spec(expr) -> OperationCallbacksSpec | None:
    try:
        return object.__getattribute__(expr, _OPERATION_CALLBACKS_ATTR)
    except AttributeError:
        return None


def _retokenize_expr_for_spec(expr, spec: OperationCallbacksSpec):
    token = _tokenize_deterministic(
        expr.deterministic_token,
        spec.operation_id,
        spec.conflict_key(),
    )
    return type(expr)(*expr.operands, _determ_token=token)


def _normalize_copy_mode(copy_mode: str, *, field_name: str) -> str:
    if not isinstance(copy_mode, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = copy_mode.strip().lower()
    if normalized not in _COPY_MODES:
        raise ValueError(
            f"{field_name} must be one of {sorted(_COPY_MODES)}, got {copy_mode!r}"
        )
    return normalized


def _normalize_partition_dispatch_mode(partition_dispatch_mode: str) -> str:
    if not isinstance(partition_dispatch_mode, str):
        raise TypeError("partition_dispatch_mode must be a string")
    normalized = partition_dispatch_mode.strip().lower()
    if normalized not in _PARTITION_DISPATCH_MODES:
        raise ValueError(
            "partition_dispatch_mode must be one of "
            f"{sorted(_PARTITION_DISPATCH_MODES)}, got {partition_dispatch_mode!r}"
        )
    return normalized


def _copy_value(value: Any, copy_mode: str) -> Any:
    if copy_mode == "none":
        return value
    if not hasattr(value, "copy"):
        return value
    if copy_mode == "shallow":
        try:
            return value.copy()
        except Exception:
            return value
    try:
        return value.copy(deep=True)
    except TypeError:
        try:
            return value.copy()
        except Exception:
            return value
    except Exception:
        return value


def _runtime_metadata_identity(metadata: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    pairs = ((str(key), id(value)) for key, value in metadata.items())
    return tuple(sorted(pairs))


class _PartitionCallbackDispatcher:
    _STOP = object()

    def __init__(
        self,
        *,
        invoke_callback: Callable[[OperationCallbacksSpec, Any, OperationMeta], None],
        max_workers: int,
        queue_size: int,
    ) -> None:
        self._invoke_callback = invoke_callback
        self._queue: Queue = Queue(maxsize=queue_size)
        self._workers: list[Thread] = []
        self._error_lock = Lock()
        self._error: BaseException | None = None
        self._operation_pending: dict[Any, int] = {}
        self._operation_pending_condition = Condition(Lock())
        self._closed = False
        for index in range(max_workers):
            worker = Thread(
                target=self._worker_loop,
                name=f"DaskPartitionCallback-{index}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    def _set_error(self, exc: BaseException) -> None:
        with self._error_lock:
            if self._error is None:
                self._error = exc

    def _get_error(self) -> BaseException | None:
        with self._error_lock:
            return self._error

    def _raise_if_failed(self) -> None:
        exc = self._get_error()
        if exc is None:
            return
        raise RuntimeError("Asynchronous partition callback failed") from exc

    def _increment_operation_pending(self, operation_id: Any) -> None:
        with self._operation_pending_condition:
            current = self._operation_pending.get(operation_id, 0)
            self._operation_pending[operation_id] = current + 1

    def _decrement_operation_pending(self, operation_id: Any) -> None:
        with self._operation_pending_condition:
            remaining = self._operation_pending.get(operation_id, 0) - 1
            if remaining <= 0:
                self._operation_pending.pop(operation_id, None)
            else:
                self._operation_pending[operation_id] = remaining
            self._operation_pending_condition.notify_all()

    def _worker_loop(self) -> None:
        while True:
            item = self._queue.get()
            operation_id = None
            try:
                if item is self._STOP:
                    return
                spec, result, op_meta = item
                operation_id = op_meta.operation_id
                if self._get_error() is not None:
                    continue
                self._invoke_callback(spec, result, op_meta)
            except BaseException as exc:  # noqa: BLE001
                self._set_error(exc)
            finally:
                if operation_id is not None:
                    self._decrement_operation_pending(operation_id)
                self._queue.task_done()

    def submit(self, spec: OperationCallbacksSpec, result: Any, op_meta: OperationMeta) -> None:
        self._raise_if_failed()
        self._increment_operation_pending(op_meta.operation_id)
        self._queue.put((spec, result, op_meta))
        self._raise_if_failed()

    def wait_operation(self, operation_id: Any) -> None:
        while True:
            self._raise_if_failed()
            with self._operation_pending_condition:
                if self._operation_pending.get(operation_id, 0) == 0:
                    break
                self._operation_pending_condition.wait(timeout=0.05)
        self._raise_if_failed()

    def close(self, *, raise_on_error: bool) -> None:
        if self._closed:
            if raise_on_error:
                self._raise_if_failed()
            return
        self._queue.join()
        for _ in self._workers:
            self._queue.put(self._STOP)
        for worker in self._workers:
            worker.join()
        self._closed = True
        if raise_on_error:
            self._raise_if_failed()


@dataclass(eq=False, frozen=True)
class OperationCallbacksSpec:
    operation_id: Any
    operation_type: str
    ddf_meta: Any
    on_start: Any | None = None
    on_end: Any | None = None
    on_partition: Any | None = None
    on_error: Any | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    metadata_token: Any | None = None
    operation_id_explicit: bool = False
    copy_meta_mode: str = "deep"
    copy_partition_mode: str = "deep"
    partition_dispatch_mode: str = "sync"
    partition_dispatch_workers: int = _DEFAULT_PARTITION_DISPATCH_WORKERS
    partition_dispatch_queue_size: int = _DEFAULT_PARTITION_DISPATCH_QUEUE_SIZE
    _conflict_key: tuple[Any, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        copy_meta_mode = _normalize_copy_mode(
            self.copy_meta_mode,
            field_name="copy_meta_mode",
        )
        copy_partition_mode = _normalize_copy_mode(
            self.copy_partition_mode,
            field_name="copy_partition_mode",
        )
        partition_dispatch_mode = _normalize_partition_dispatch_mode(
            self.partition_dispatch_mode
        )
        if not isinstance(self.partition_dispatch_workers, int):
            raise TypeError("partition_dispatch_workers must be an int")
        if self.partition_dispatch_workers <= 0:
            raise ValueError("partition_dispatch_workers must be > 0")
        if not isinstance(self.partition_dispatch_queue_size, int):
            raise TypeError("partition_dispatch_queue_size must be an int")
        if self.partition_dispatch_queue_size <= 0:
            raise ValueError("partition_dispatch_queue_size must be > 0")

        if self.metadata_token is not None:
            metadata_conflict_key = (
                "metadata_token",
                _tokenize_deterministic(self.metadata_token),
            )
        elif self.operation_id_explicit:
            metadata_conflict_key = (
                "runtime_metadata_identity",
                _runtime_metadata_identity(self.metadata),
            )
        else:
            metadata_conflict_key = ("generated_operation_id", self.operation_id)

        conflict_key = (
            self.operation_type,
            id(self.on_start),
            id(self.on_end),
            id(self.on_partition),
            id(self.on_error),
            metadata_conflict_key,
            copy_meta_mode,
            copy_partition_mode,
            partition_dispatch_mode,
            self.partition_dispatch_workers,
            self.partition_dispatch_queue_size,
        )
        object.__setattr__(self, "copy_meta_mode", copy_meta_mode)
        object.__setattr__(self, "copy_partition_mode", copy_partition_mode)
        object.__setattr__(self, "partition_dispatch_mode", partition_dispatch_mode)
        object.__setattr__(self, "_conflict_key", conflict_key)

    def to_operation_meta(self, partition_idx: int, partition_count: int) -> OperationMeta:
        return OperationMeta(
            operation_id=self.operation_id,
            operation_type=self.operation_type,
            partition_idx=partition_idx,
            partition_count=partition_count,
            operation_task_count=partition_count,
        )

    def conflict_key(self) -> tuple[Any, ...]:
        return self._conflict_key


def build_operation_callbacks_spec(
    *,
    ddf_meta: Any,
    operation_type: str,
    on_start: Any | None = None,
    on_end: Any | None = None,
    on_partition: Any | None = None,
    on_error: Any | None = None,
    metadata: dict[str, Any] | None = None,
    metadata_token: Any | None = None,
    operation_id: Any | None = None,
    operation_token: Any | None = None,
    copy_meta_mode: str = "deep",
    copy_partition_mode: str = "deep",
    partition_dispatch_mode: str = "sync",
    partition_dispatch_workers: int = _DEFAULT_PARTITION_DISPATCH_WORKERS,
    partition_dispatch_queue_size: int = _DEFAULT_PARTITION_DISPATCH_QUEUE_SIZE,
) -> OperationCallbacksSpec | None:
    if not isinstance(operation_type, str) or not operation_type:
        raise TypeError("operation_type must be a non-empty string")

    copy_meta_mode = _normalize_copy_mode(copy_meta_mode, field_name="copy_meta_mode")
    copy_partition_mode = _normalize_copy_mode(
        copy_partition_mode,
        field_name="copy_partition_mode",
    )
    partition_dispatch_mode = _normalize_partition_dispatch_mode(partition_dispatch_mode)

    if metadata is None:
        metadata_dict: dict[str, Any] = {}
    elif isinstance(metadata, dict):
        metadata_dict = dict(metadata)
    else:
        raise TypeError(
            f"metadata must be a dict[str, Any] or None, got {type(metadata).__name__}"
        )

    if reserved := _RESERVED_METADATA_KEYS & metadata_dict.keys():
        raise ValueError(
            "metadata contains reserved keys that are injected by the callback bridge: "
            + ", ".join(sorted(reserved))
        )

    if (
        on_start is None
        and on_end is None
        and on_partition is None
        and on_error is None
    ):
        return None

    operation_id_explicit = operation_id is not None
    if operation_id is None:
        token_payload = metadata_token if metadata_token is not None else metadata_dict
        token = _tokenize_deterministic(operation_type, operation_token, token_payload)
        operation_id = f"{operation_type}-{token}"

    return OperationCallbacksSpec(
        operation_id=operation_id,
        operation_type=operation_type,
        ddf_meta=ddf_meta,
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        on_error=on_error,
        metadata=metadata_dict,
        metadata_token=metadata_token,
        operation_id_explicit=operation_id_explicit,
        copy_meta_mode=copy_meta_mode,
        copy_partition_mode=copy_partition_mode,
        partition_dispatch_mode=partition_dispatch_mode,
        partition_dispatch_workers=partition_dispatch_workers,
        partition_dispatch_queue_size=partition_dispatch_queue_size,
    )


def set_expr_operation_callbacks_spec(expr, spec: OperationCallbacksSpec | None):
    if spec is None:
        return expr
    existing = get_expr_operation_callbacks_spec(expr)
    if existing is not None and (
        existing.operation_id != spec.operation_id
        or existing.conflict_key() != spec.conflict_key()
    ):
        # Some expressions are singletons and may be reused in unrelated branches.
        # Retokenize to isolate callback-spec attachment from previously annotated uses.
        expr = _retokenize_expr_for_spec(expr, spec)
        existing = get_expr_operation_callbacks_spec(expr)
        if existing is not None and (
            existing.operation_id != spec.operation_id
            or existing.conflict_key() != spec.conflict_key()
        ):
            raise RuntimeError(
                f"Expression {expr!r} already has a different operation callbacks spec attached."
            )
    object.__setattr__(expr, _OPERATION_CALLBACKS_ATTR, spec)
    return expr


def get_expr_operation_callbacks_spec(expr) -> OperationCallbacksSpec | None:
    return _get_operation_callbacks_spec(expr)


def collect_operation_callbacks_specs(expr) -> list[OperationCallbacksSpec]:
    by_operation_id: dict[Any, OperationCallbacksSpec] = {}
    for node in expr.walk():
        spec = get_expr_operation_callbacks_spec(node)
        if spec is None:
            continue
        existing = by_operation_id.get(spec.operation_id)
        if existing is not None:
            if existing.conflict_key() != spec.conflict_key():
                raise RuntimeError(
                    f"Conflicting callback specs for operation_id={spec.operation_id!r}"
                )
            continue
        by_operation_id[spec.operation_id] = spec
    return list(by_operation_id.values())


class PublicOperationCallbacks(Callback):
    def __init__(self, specs: list[OperationCallbacksSpec]):
        by_operation_id = {}
        for spec in specs:
            existing = by_operation_id.get(spec.operation_id)
            if existing is not None and existing.conflict_key() != spec.conflict_key():
                raise RuntimeError(
                    f"Conflicting callback specs for operation_id={spec.operation_id!r}"
                )
            by_operation_id[spec.operation_id] = spec
        self._specs_by_operation_id = by_operation_id
        threaded_specs = [
            spec
            for spec in by_operation_id.values()
            if spec.partition_dispatch_mode == "threaded" and spec.on_partition is not None
        ]
        if threaded_specs:
            self._threaded_dispatcher_config = (
                max(spec.partition_dispatch_workers for spec in threaded_specs),
                max(spec.partition_dispatch_queue_size for spec in threaded_specs),
            )
        else:
            self._threaded_dispatcher_config = None
        self._threaded_dispatcher: _PartitionCallbackDispatcher | None = None
        super().__init__()

    def _spec_for_operation_id(self, operation_id: Any) -> OperationCallbacksSpec:
        try:
            return self._specs_by_operation_id[operation_id]
        except KeyError as exc:
            raise RuntimeError(
                f"Missing callback spec for operation_id={operation_id!r}"
            ) from exc

    def _get_or_create_threaded_dispatcher(self) -> _PartitionCallbackDispatcher:
        if self._threaded_dispatcher is not None:
            return self._threaded_dispatcher
        if self._threaded_dispatcher_config is None:
            raise RuntimeError("Threaded partition dispatcher is not configured")
        max_workers, queue_size = self._threaded_dispatcher_config
        self._threaded_dispatcher = _PartitionCallbackDispatcher(
            invoke_callback=self._invoke_partition_callback,
            max_workers=max_workers,
            queue_size=queue_size,
        )
        return self._threaded_dispatcher

    def _invoke_partition_callback(
        self,
        spec: OperationCallbacksSpec,
        result: Any,
        op_meta: OperationMeta,
    ) -> None:
        if spec.on_partition is None:
            return
        partition_info = {
            "number": op_meta.partition_idx,
            "partition_count": op_meta.partition_count,
            "stage": "finish",
        }
        spec.on_partition(
            _copy_value(result, spec.copy_partition_mode),
            spec.operation_id,
            **spec.metadata,
            partition_info=partition_info,
        )

    def _operation_start(self, op_meta: OperationMeta, dsk, state):
        spec = self._spec_for_operation_id(op_meta.operation_id)
        if spec.on_start is None:
            return
        spec.on_start(
            _copy_value(spec.ddf_meta, spec.copy_meta_mode),
            spec.operation_id,
            **spec.metadata,
        )

    def _operation_end(self, op_meta: OperationMeta, dsk, state):
        spec = self._spec_for_operation_id(op_meta.operation_id)
        if spec.on_end is None:
            return
        if spec.partition_dispatch_mode == "threaded":
            dispatcher = self._threaded_dispatcher
            if dispatcher is not None:
                dispatcher.wait_operation(spec.operation_id)
        spec.on_end(
            _copy_value(spec.ddf_meta, spec.copy_meta_mode),
            spec.operation_id,
            **spec.metadata,
        )

    def _operation_error(self, op_meta: OperationMeta, exc: BaseException, dsk, state):
        spec = self._spec_for_operation_id(op_meta.operation_id)
        if spec.on_error is None:
            return
        spec.on_error(
            _copy_value(spec.ddf_meta, spec.copy_meta_mode),
            spec.operation_id,
            exc,
            **spec.metadata,
        )

    def _posttask(self, key, result, dsk, state, worker_id):
        node = dsk.get(key)
        op_meta = getattr(node, "op_meta", None)
        if op_meta is None:
            return
        spec = self._spec_for_operation_id(op_meta.operation_id)
        if spec.on_partition is None:
            return
        if spec.partition_dispatch_mode == "sync":
            self._invoke_partition_callback(spec, result, op_meta)
            return
        dispatcher = self._get_or_create_threaded_dispatcher()
        dispatcher.submit(spec, result, op_meta)

    def _finish(self, dsk, state, failed):
        dispatcher = self._threaded_dispatcher
        if dispatcher is None:
            return
        dispatcher.close(raise_on_error=not failed)
