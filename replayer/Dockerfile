FROM python:3.12-slim
WORKDIR /app
COPY replayer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY replayer/src ./src
COPY common/python/realtime_lab ./realtime_lab
RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER appuser
CMD [ "python", "-m", "src.replay_dlq" ]
