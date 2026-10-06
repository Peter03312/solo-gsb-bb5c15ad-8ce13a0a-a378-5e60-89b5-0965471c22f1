FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PORT=8000 \
    APP_URL=http://localhost:8000 \
    DB_PATH=/app/data/scheduler.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/
COPY static/ static/

RUN mkdir -p /app/data
EXPOSE 8000

CMD ["python", "app.py"]
