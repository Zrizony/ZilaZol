# Use small, secure Python image
FROM python:3.11-slim AS app

# Avoid Python cache files, enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system packages if needed (only what's required)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium

# Copy the entire app code
COPY . .

# Install Playwright browsers
RUN playwright install chromium

# Create a non-root user for Cloud Run security
RUN useradd -m appuser
USER appuser

# Run the Flask app
CMD ["python", "app.py"]
