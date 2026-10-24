FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY aqi_exporter.py .

RUN useradd --uid 10001 --no-create-home app
USER app

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/metrics', timeout=3)"
ENTRYPOINT ["python", "-u", "aqi_exporter.py"]
