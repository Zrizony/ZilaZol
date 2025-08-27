# Use Ubuntu 22.04 with Python 3.11
FROM ubuntu:22.04

# Set working directory
WORKDIR /app

# Set timezone to avoid interactive prompt
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

# Install Python and system dependencies
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get update && apt-get install -y \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y \
    python3.11 \
    python3.11-venv \
    python3-pip \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-unifont \
    fonts-ubuntu \
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
    libgdk-pixbuf2.0-0 \
    libjpeg-turbo8 \
    libwebp7 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Create symlinks for python and pip
RUN rm -f /usr/bin/python /usr/bin/pip && \
    ln -s /usr/bin/python3.11 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN ls -la /app/
RUN cat /app/requirements.txt
RUN pip --version
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install playwright
RUN echo "=== Checking installed packages ==="
RUN pip list
RUN echo "=== Checking if Flask was installed ==="
RUN pip list | grep -i flask || echo "Flask not found in pip list"
RUN python -c "import flask; print('Flask version:', flask.__version__)" || echo "Flask import failed"

# Copy application code and test Flask installation
COPY . .
RUN python test_flask_install.py

# Install Playwright browsers with explicit path
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN playwright install chromium

# Set environment variables
ENV PYTHONPATH=/app
ENV DISPLAY=:99

# Expose port
EXPOSE 8080

# Run the Flask app
CMD ["python", "app.py"] 