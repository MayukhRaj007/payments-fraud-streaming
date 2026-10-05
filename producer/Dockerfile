FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY profiles.py anomalies.py simulator.py ./

# Labels (ground truth) go to a volume shared with scripts/evaluate.py.
VOLUME ["/data"]

# Unbuffered so `docker compose logs -f producer` is live rather than chunked.
ENV PYTHONUNBUFFERED=1
CMD ["python", "simulator.py"]
