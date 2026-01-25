FROM python:3.11-slim

# System packages (only if you actually need them)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install app dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copy project files
COPY . /app

# Environment settings
ENV TZ=Europe/London

# Railway sets $PORT automatically
CMD gunicorn "gunicorn_run:app" \
    -b 0.0.0.0:${PORT:-82} \
    -w 2 -k gthread --threads 8 --timeout 120
