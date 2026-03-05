from __future__ import annotations

import threading

import pytest

from dask.dataframe.dask_expr import concat, from_pandas, merge
from dask.dataframe.dask_expr._operation_callbacks import (
    build_operation_callbacks_spec,
    collect_operation_callbacks_specs,
)
from dask.dataframe.dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


def _noop(*args, **kwargs):
    return None


def test_build_operation_callbacks_spec():
    meta = pd.DataFrame({"a": [1]}).head(0)
    spec = build_operation_callbacks_spec(
        ddf_meta=meta,
        operation_type="merge",
        on_partition=_noop,
        metadata={"node_id": "merge_1"},
        operation_token="token-1",
    )
    assert spec is not None
    assert spec.operation_type == "merge"
    assert spec.metadata == {"node_id": "merge_1"}

    spec2 = build_operation_callbacks_spec(
        ddf_meta=meta,
        operation_type="merge",
        on_partition=_noop,
        metadata={"node_id": "merge_1"},
        operation_token="token-1",
    )
    assert spec2 is not None
    assert spec.operation_id == spec2.operation_id

    with pytest.raises(TypeError, match="metadata must be a dict"):
        build_operation_callbacks_spec(
            ddf_meta=meta,
            operation_type="merge",
            on_partition=_noop,
            metadata=["bad"],  # type: ignore[arg-type]
        )

    spec_error_only = build_operation_callbacks_spec(
        ddf_meta=meta,
        operation_type="merge",
        on_error=_noop,
        metadata={"node_id": "merge_error"},
        operation_token="token-err",
    )
    assert spec_error_only is not None

    with pytest.raises(ValueError, match="reserved keys"):
        build_operation_callbacks_spec(
            ddf_meta=meta,
            operation_type="merge",
            on_error=_noop,
            metadata={"exc": "bad"},
        )

    runtime_metadata_spec = build_operation_callbacks_spec(
        ddf_meta=meta,
        operation_type="merge",
        on_partition=_noop,
        metadata={"runtime_ctx": object()},
        operation_id="merge_with_runtime_metadata",
    )
    assert runtime_metadata_spec is not None
    assert runtime_metadata_spec.operation_id == "merge_with_runtime_metadata"

    metadata_token_spec = build_operation_callbacks_spec(
        ddf_meta=meta,
        operation_type="merge",
        on_partition=_noop,
        metadata={"runtime_ctx": object()},
        metadata_token="stable-metadata-token",
        operation_token="token-with-metadata-token",
    )
    assert metadata_token_spec is not None
    assert metadata_token_spec.operation_id.startswith("merge-")

    assert metadata_token_spec.conflict_key() is metadata_token_spec.conflict_key()


def test_collect_operation_callbacks_specs_and_conflicts():
    pdf = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    ddf = from_pandas(pdf, npartitions=2)

    one = ddf.filter_rows(
        ddf["a"] > 0,
        on_partition=_noop,
        metadata={"node_id": "one"},
        operation_id="filter_op_one",
    )
    specs = collect_operation_callbacks_specs(one.expr)
    assert len(specs) == 1
    assert specs[0].operation_id == "filter_op_one"

    left = ddf.filter_rows(
        ddf["a"] > 0,
        on_partition=_noop,
        metadata={"node_id": "left"},
        operation_id="dup_op",
    )
    right = ddf.filter_rows(
        ddf["b"] > 0,
        on_partition=_noop,
        metadata={"node_id": "right"},
        operation_id="dup_op",
    )
    conflicted = concat([left, right], axis=0)
    with pytest.raises(RuntimeError, match="Conflicting callback specs"):
        collect_operation_callbacks_specs(conflicted.expr)


def test_operation_callbacks_end_to_end_and_result_safety():
    orders_pdf = pd.DataFrame(
        {
            "order_id": list(range(40)),
            "product_id": [1, 2, 3, 4] * 10,
            "amount": [1.0, 2.0, 3.0, 4.0] * 10,
        }
    )
    products_pdf = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["A", "A", "B", "C"],
            "price": [10.0, 20.0, 7.5, 99.0],
        }
    )

    orders = from_pandas(orders_pdf, npartitions=4)
    products = from_pandas(products_pdf, npartitions=1)

    events = {"start": [], "end": [], "partition": []}

    def on_start(ddf_meta, operation_id, marker):
        events["start"].append((operation_id, marker, tuple(ddf_meta.columns)))

    def on_end(ddf_meta, operation_id, marker):
        events["end"].append((operation_id, marker, tuple(ddf_meta.columns)))

    def on_partition(df_partition, operation_id, marker, partition_info):
        events["partition"].append(
            (
                operation_id,
                marker,
                partition_info["number"],
                partition_info["partition_count"],
                partition_info["stage"],
            )
        )
        if hasattr(df_partition, "__setitem__") and getattr(df_partition, "ndim", 0) == 2:
            # Mutation here should not leak into the real compute result.
            df_partition["__callback_mutation__"] = 1

    ddf = merge(
        orders,
        products,
        on="product_id",
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        operation_id="merge_orders_products",
        metadata={"marker": "merge"},
    )
    ddf = ddf.filter_rows(
        ddf["amount"] > 1,
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        operation_id="filter_amount_gt_1",
        metadata={"marker": "filter"},
    )
    ddf = ddf.groupby("category").agg(
        {"amount": ["mean"], "price": ["mean"]},
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        operation_id="agg_by_category",
        metadata={"marker": "agg"},
    )

    # Build expected with native pandas pipeline to avoid callback side effects.
    expected_pd = orders_pdf.merge(products_pdf, on="product_id")
    expected_pd = expected_pd[expected_pd["amount"] > 1]
    expected_pd = expected_pd.groupby("category").agg(
        {"amount": ["mean"], "price": ["mean"]}
    )

    result = ddf.compute(scheduler="threads")
    assert_eq(result, expected_pd)

    for operation_id in [
        "merge_orders_products",
        "filter_amount_gt_1",
        "agg_by_category",
    ]:
        starts = [e for e in events["start"] if e[0] == operation_id]
        ends = [e for e in events["end"] if e[0] == operation_id]
        partitions = [e for e in events["partition"] if e[0] == operation_id]
        assert len(starts) == 1
        assert len(ends) == 1
        assert partitions
        partition_count_values = {p[3] for p in partitions}
        assert len(partition_count_values) == 1
        partition_count = partition_count_values.pop()
        assert len({p[2] for p in partitions}) == partition_count
        assert all(p[4] == "finish" for p in partitions)


def test_operation_callbacks_on_error_public_api():
    pdf = pd.DataFrame({"a": [1, 2, 3, 4]})
    ddf = from_pandas(pdf, npartitions=2)

    events = {"start": 0, "end": 0, "error": []}

    def on_start(ddf_meta, operation_id, marker):
        assert hasattr(ddf_meta, "columns")
        assert operation_id == "filter_bad_predicate"
        assert marker == "bad_predicate"
        events["start"] += 1

    def on_end(ddf_meta, operation_id, marker):
        events["end"] += 1

    def on_error(ddf_meta, operation_id, exc, marker):
        events["error"].append(
            (
                operation_id,
                marker,
                type(exc).__name__,
                tuple(ddf_meta.columns),
            )
        )

    bad = ddf.filter_rows(
        ddf["a"],
        on_start=on_start,
        on_end=on_end,
        on_error=on_error,
        operation_id="filter_bad_predicate",
        metadata={"marker": "bad_predicate"},
    )

    with pytest.raises(KeyError):
        bad.compute(scheduler="threads")

    assert events["start"] == 1
    assert events["end"] == 0
    assert len(events["error"]) == 1
    operation_id, marker, exc_name, columns = events["error"][0]
    assert operation_id == "filter_bad_predicate"
    assert marker == "bad_predicate"
    assert exc_name == "KeyError"
    assert isinstance(columns, tuple)


def test_add_callbacks_chainable_api_end_to_end():
    orders_pdf = pd.DataFrame(
        {
            "order_id": list(range(40)),
            "product_id": [1, 2, 3, 4] * 10,
            "amount": [1.0, 2.0, 3.0, 4.0] * 10,
        }
    )
    products_pdf = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["A", "A", "B", "C"],
            "price": [10.0, 20.0, 7.5, 99.0],
        }
    )

    orders = from_pandas(orders_pdf, npartitions=4)
    products = from_pandas(products_pdf, npartitions=1)

    events = {"start": [], "end": [], "partition": []}

    def on_start(ddf_meta, operation_id, marker):
        events["start"].append((operation_id, marker, tuple(ddf_meta.columns)))

    def on_end(ddf_meta, operation_id, marker):
        events["end"].append((operation_id, marker, tuple(ddf_meta.columns)))

    def on_partition(df_partition, operation_id, marker, partition_info):
        events["partition"].append(
            (
                operation_id,
                marker,
                partition_info["number"],
                partition_info["partition_count"],
                partition_info["stage"],
            )
        )
        if hasattr(df_partition, "__setitem__") and getattr(df_partition, "ndim", 0) == 2:
            df_partition["__callback_mutation__"] = 1

    ddf = merge(orders, products, on="product_id").add_callbacks(
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        operation_id="merge_orders_products_v2",
        metadata={"marker": "merge"},
    )
    ddf = ddf.filter_rows(ddf["amount"] > 1).add_callbacks(
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        operation_id="filter_amount_gt_1_v2",
        metadata={"marker": "filter"},
    )
    ddf = ddf.groupby("category").agg(
        {"amount": ["mean"], "price": ["mean"]}
    ).add_callbacks(
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        operation_id="agg_by_category_v2",
        metadata={"marker": "agg"},
    )

    expected_pd = orders_pdf.merge(products_pdf, on="product_id")
    expected_pd = expected_pd[expected_pd["amount"] > 1]
    expected_pd = expected_pd.groupby("category").agg(
        {"amount": ["mean"], "price": ["mean"]}
    )
    result = ddf.compute(scheduler="threads")
    assert_eq(result, expected_pd)

    for operation_id in [
        "merge_orders_products_v2",
        "filter_amount_gt_1_v2",
        "agg_by_category_v2",
    ]:
        starts = [e for e in events["start"] if e[0] == operation_id]
        ends = [e for e in events["end"] if e[0] == operation_id]
        partitions = [e for e in events["partition"] if e[0] == operation_id]
        assert len(starts) == 1
        assert len(ends) == 1
        assert partitions
        partition_count_values = {p[3] for p in partitions}
        assert len(partition_count_values) == 1
        partition_count = partition_count_values.pop()
        assert len({p[2] for p in partitions}) == partition_count
        assert all(p[4] == "finish" for p in partitions)


def test_add_callbacks_chainable_api_on_error():
    pdf = pd.DataFrame({"a": [1, 2, 3, 4]})
    ddf = from_pandas(pdf, npartitions=2)

    events = {"start": 0, "end": 0, "error": []}

    def on_start(ddf_meta, operation_id, marker):
        assert hasattr(ddf_meta, "columns")
        assert operation_id == "filter_bad_predicate_v2"
        assert marker == "bad_predicate"
        events["start"] += 1

    def on_end(ddf_meta, operation_id, marker):
        events["end"] += 1

    def on_error(ddf_meta, operation_id, exc, marker):
        events["error"].append(
            (
                operation_id,
                marker,
                type(exc).__name__,
                tuple(ddf_meta.columns),
            )
        )

    bad = (
        ddf.filter_rows(ddf["a"])
        .add_callbacks(
            on_start=on_start,
            on_end=on_end,
            on_error=on_error,
            operation_id="filter_bad_predicate_v2",
            metadata={"marker": "bad_predicate"},
        )
    )

    with pytest.raises(KeyError):
        bad.compute(scheduler="threads")

    assert events["start"] == 1
    assert events["end"] == 0
    assert len(events["error"]) == 1
    operation_id, marker, exc_name, columns = events["error"][0]
    assert operation_id == "filter_bad_predicate_v2"
    assert marker == "bad_predicate"
    assert exc_name == "KeyError"
    assert isinstance(columns, tuple)


def test_operation_callback_specs_survive_optimization_and_skip_fusion():
    left_pdf = pd.DataFrame({"k": [1, 2, 3, 4], "v1": [1, 2, 3, 4]})
    right_pdf = pd.DataFrame({"k": [1, 2, 3, 4], "v2": [10, 20, 30, 40]})
    left = from_pandas(left_pdf, npartitions=2)
    right = from_pandas(right_pdf, npartitions=2)

    ddf = left.merge(
        right,
        on="k",
        on_partition=_noop,
        operation_id="merge_cb",
        metadata={"marker": "merge"},
    )

    for optimized_expr in [
        ddf.expr.simplify(),
        ddf.expr.lower_completely(),
        ddf.expr.optimize(fuse=True),
    ]:
        specs = collect_operation_callbacks_specs(optimized_expr)
        assert len(specs) == 1
        assert specs[0].operation_id == "merge_cb"

    optimized = ddf.expr.optimize(fuse=True)
    assert all("Fused" not in type(node).__name__ for node in optimized.walk())


def test_public_callbacks_copy_partition_mode():
    pdf = pd.DataFrame({"a": [1, 2, 3, 4]})

    base_without_copy = from_pandas(pdf, npartitions=2)
    mutated_without_copy = base_without_copy.filter_rows(
        base_without_copy["a"] > 0,
        on_partition=lambda df_partition, *_args, **_kwargs: df_partition.__setitem__(
            "__callback_mutation__", 1
        ),
        operation_id="copy_none",
        copy_partition_mode="none",
    )
    result_without_copy = mutated_without_copy.compute(scheduler="threads")
    assert "__callback_mutation__" in result_without_copy.columns

    base_with_copy = from_pandas(pdf, npartitions=2)
    protected_with_copy = base_with_copy.filter_rows(
        base_with_copy["a"] > 0,
        on_partition=lambda df_partition, *_args, **_kwargs: df_partition.__setitem__(
            "__callback_mutation__", 1
        ),
        operation_id="copy_deep",
        copy_partition_mode="deep",
    )
    result_with_copy = protected_with_copy.compute(scheduler="threads")
    assert "__callback_mutation__" not in result_with_copy.columns


def test_public_callbacks_threaded_partition_dispatch():
    pdf = pd.DataFrame({"a": list(range(20))})
    base = from_pandas(pdf, npartitions=5)
    callback_thread_ids = set()
    callback_lock = threading.Lock()
    scheduler_thread_id = threading.get_ident()

    def on_partition(_df_partition, _operation_id, **_kwargs):
        with callback_lock:
            callback_thread_ids.add(threading.get_ident())

    ddf = base.filter_rows(
        base["a"] >= 0,
        on_partition=on_partition,
        operation_id="threaded_dispatch",
        partition_dispatch_mode="threaded",
        partition_dispatch_workers=2,
        partition_dispatch_queue_size=8,
    )

    result = ddf.compute(scheduler="threads")
    assert len(result) == len(pdf)
    assert callback_thread_ids
    assert all(thread_id != scheduler_thread_id for thread_id in callback_thread_ids)


def test_public_callbacks_disallow_distributed_scheduler():
    pdf = pd.DataFrame({"a": [1, 2, 3]})
    base = from_pandas(pdf, npartitions=2)
    ddf = base.filter_rows(base["a"] > 1, on_partition=_noop)
    with pytest.raises(RuntimeError):
        ddf.compute(scheduler="distributed")
