# syntax=docker/dockerfile:1.7

FROM python:3.13-slim AS base

ENV PYTHONPATH="${PYTHONPATH}:/app" \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY . /app

RUN python -m pip install --upgrade pip \
    && python -m pip install -e ".[complete]" \
    && python -m pip install psutil

RUN chmod +x /app/services/benchmarking/docker/entrypoint.sh

ENTRYPOINT ["/app/services/benchmarking/docker/entrypoint.sh"]
CMD ["python", "-m", "services.benchmarking.runner"]
