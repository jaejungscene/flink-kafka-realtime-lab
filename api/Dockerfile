FROM python:3.14-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/src ./src
COPY common/python/realtime_lab ./realtime_lab

FROM base AS test
COPY api/requirements-test.txt .
RUN pip install --no-cache-dir -r requirements-test.txt
COPY pyproject.toml .
COPY api/tests ./tests
RUN ruff check src realtime_lab tests \
    && PYTHONPATH=/app python -m unittest discover -s tests

FROM base AS runtime
RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER appuser
CMD [ "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000" ]
