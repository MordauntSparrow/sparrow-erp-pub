#!/bin/sh
set -e
# Railway (and similar) set PORT; nginx listens here. run_website.py must use a different port (default 80).
PORT="${PORT:-8080}"
INSTALL_DEFAULT="/etc/nginx/sites-available/default"
mkdir -p /etc/nginx/sites-available /etc/nginx/sites-enabled /var/lib/nginx/body /var/log/nginx
sed \
  -e "s/listen 80 default_server/listen ${PORT} default_server/" \
  -e "s/listen 80;/listen ${PORT};/" \
  /app/nginx.conf > "$INSTALL_DEFAULT"
ln -sf "$INSTALL_DEFAULT" /etc/nginx/sites-enabled/default
nginx -t
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
