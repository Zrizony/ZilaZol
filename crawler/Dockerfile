# Use small, secure Python image
FROM python:3.11-slim AS app

# Avoid Python cache files, enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies required for Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    # Playwright dependencies
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 libpango-1.0-0 libcairo2 \
 && rm -rf /var/lib/apt/lists/*

# Create a non-root user FIRST (before installing anything)
RUN useradd -m appuser

# Set working directory
WORKDIR /app

# Change ownership to appuser
RUN chown -R appuser:appuser /app

# Switch to appuser for all subsequent operations
USER appuser

# Install Python dependencies as appuser
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --user --upgrade pip && pip install --user -r requirements.txt

# Add user's local bin to PATH
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Install Playwright browsers as appuser
RUN playwright install chromium

# Copy the entire app code as appuser
COPY --chown=appuser:appuser . .

# Run the Flask app
CMD ["python", "app.py"]
