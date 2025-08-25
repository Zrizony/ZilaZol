# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (minimal but complete)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-unifont \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    libgdk-pixbuf-xlib-2.0-0 \
    libjpeg62-turbo \
    libwebp7 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install playwright

# Install Playwright browsers with explicit path
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV DISPLAY=:99

# Expose port
EXPOSE 8080

# Run the Flask app
CMD ["python", "app.py"] 