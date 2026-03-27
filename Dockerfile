FROM python:3.11-slim

WORKDIR /app

# Install nginx, supervisor, and build dependencies for Python packages like mysqlclient
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx supervisor curl unzip gcc default-libmysqlclient-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Copy nginx config into place
COPY nginx.conf /etc/nginx/sites-available/default

# Copy supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose nginx port (Railway will map this to the public URL)
EXPOSE 80

# Start supervisor (which manages nginx and your Python processes)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
