from .config import BenchmarkConfig



def run_benchmark(*args, **kwargs):
    from .runner import run_benchmark as _run_benchmark

    return _run_benchmark(*args, **kwargs)


__all__ = ["BenchmarkConfig", "run_benchmark"]
