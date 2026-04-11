# Sparrow ERP — production-style image (nginx + supervisord + admin + public website)
# Railway: PORT is injected at runtime; entrypoint rewrites nginx listen directives.

FROM python:3.10-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8080

# System: nginx + supervisor + WeasyPrint stack + common build deps for pip wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    supervisor \
    build-essential \
    pkg-config \
    libffi-dev \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf2.0-0 \
    libjpeg62-turbo \
    libopenjp2-7 \
    shared-mime-info \
    tesseract-ocr \
    tesseract-ocr-eng \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# Entrypoint at repo root so it is always in the build context (some clones lacked docker/entrypoint.sh).
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
COPY . .

RUN mkdir -p /var/www/letsencrypt /app/app/logs \
    && rm -f /etc/nginx/sites-enabled/default \
    && chmod +x /app/docker-entrypoint.sh \
    && cp /app/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8080

# Railway `startCommand` overrides CMD — keep both in sync with railway.json
CMD ["/app/docker-entrypoint.sh"]
