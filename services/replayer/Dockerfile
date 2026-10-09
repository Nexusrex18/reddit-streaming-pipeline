FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.3.0
COPY replayer.py /app/
CMD ["python", "-u", "/app/replayer.py"]
