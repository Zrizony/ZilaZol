# Dockerfile
ARG BUILD_NO_CACHE=0
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

# Workdir
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Ensure browsers are available (already in base, but explicit is fine)
RUN playwright install --with-deps chromium

ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    GUNICORN_CMD_ARGS="--timeout 0 --workers 1 --threads 4 --bind 0.0.0.0:8080"

EXPOSE 8080
CMD ["gunicorn", "app:app"]
