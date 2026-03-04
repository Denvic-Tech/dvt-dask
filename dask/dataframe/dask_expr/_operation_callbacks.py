from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dask._task_spec import OperationMeta
from dask.callbacks import Callback
from dask.tokenize import _tokenize_deterministic

_OPERATION_CALLBACKS_ATTR = "__dvt_operation_callbacks_spec"
_RESERVED_METADATA_KEYS = {"ddf_meta", "operation_id", "partition_info"}


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


def _safe_copy(value: Any) -> Any:
    if hasattr(value, "copy"):
        try:
            return value.copy(deep=True)
        except TypeError:
            try:
                return value.copy()
            except Exception:
                return value
        except Exception:
            return value
    return value


@dataclass(eq=False, frozen=True)
class OperationCallbacksSpec:
    operation_id: Any
    operation_type: str
    ddf_meta: Any
    on_start: Any | None = None
    on_end: Any | None = None
    on_partition: Any | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_operation_meta(self, partition_idx: int, partition_count: int) -> OperationMeta:
        return OperationMeta(
            operation_id=self.operation_id,
            operation_type=self.operation_type,
            partition_idx=partition_idx,
            partition_count=partition_count,
            operation_task_count=partition_count,
        )

    def conflict_key(self) -> tuple[Any, ...]:
        return (
            self.operation_type,
            id(self.on_start),
            id(self.on_end),
            id(self.on_partition),
            _tokenize_deterministic(self.ddf_meta, self.metadata),
        )


def build_operation_callbacks_spec(
    *,
    ddf_meta: Any,
    operation_type: str,
    on_start: Any | None = None,
    on_end: Any | None = None,
    on_partition: Any | None = None,
    metadata: dict[str, Any] | None = None,
    operation_id: Any | None = None,
    operation_token: Any | None = None,
) -> OperationCallbacksSpec | None:
    if not isinstance(operation_type, str) or not operation_type:
        raise TypeError("operation_type must be a non-empty string")

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

    if on_start is None and on_end is None and on_partition is None:
        return None

    if operation_id is None:
        token = _tokenize_deterministic(operation_type, operation_token, metadata_dict)
        operation_id = f"{operation_type}-{token}"

    return OperationCallbacksSpec(
        operation_id=operation_id,
        operation_type=operation_type,
        ddf_meta=_safe_copy(ddf_meta),
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        metadata=metadata_dict,
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
        super().__init__()

    def _spec_for_operation_id(self, operation_id: Any) -> OperationCallbacksSpec:
        try:
            return self._specs_by_operation_id[operation_id]
        except KeyError as exc:
            raise RuntimeError(
                f"Missing callback spec for operation_id={operation_id!r}"
            ) from exc

    def _operation_start(self, op_meta: OperationMeta, dsk, state):
        spec = self._spec_for_operation_id(op_meta.operation_id)
        if spec.on_start is None:
            return
        spec.on_start(
            _safe_copy(spec.ddf_meta),
            spec.operation_id,
            **spec.metadata,
        )

    def _operation_end(self, op_meta: OperationMeta, dsk, state):
        spec = self._spec_for_operation_id(op_meta.operation_id)
        if spec.on_end is None:
            return
        spec.on_end(
            _safe_copy(spec.ddf_meta),
            spec.operation_id,
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
        partition_info = {
            "number": op_meta.partition_idx,
            "partition_count": op_meta.partition_count,
            "stage": "finish",
        }
        spec.on_partition(
            _safe_copy(result),
            spec.operation_id,
            **spec.metadata,
            partition_info=partition_info,
        )
