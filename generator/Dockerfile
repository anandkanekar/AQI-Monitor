FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sensor_simulator.py .

RUN useradd --uid 10001 --no-create-home app
USER app

ENTRYPOINT ["python", "-u", "sensor_simulator.py"]
CMD ["--bootstrap", "kafka:9092", "--sensors", "40", "--interval", "5"]
