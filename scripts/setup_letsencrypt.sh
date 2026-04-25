#!/usr/bin/env bash
# Obtain/renew Let's Encrypt certificates for control-centre.<domain>
# Usage: sudo ./scripts/setup_letsencrypt.sh yourdomain.tld
# Requires nginx and certbot installed. This script performs a webroot challenge using /var/www/letsencrypt

set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <domain> [email]"
  exit 2
fi

DOMAIN=$1
EMAIL=${2:-admin@$DOMAIN}
WEBROOT=/var/www/letsencrypt

echo "Creating webroot: $WEBROOT"
sudo mkdir -p "$WEBROOT"
sudo chown www-data:www-data "$WEBROOT"

echo "Ensuring nginx serves ACME challenge (see nginx.conf)"
sudo nginx -t
sudo systemctl reload nginx || true

echo "Requesting certificate for $DOMAIN"
sudo certbot certonly --webroot -w "$WEBROOT" -d "$DOMAIN" --agree-tos --non-interactive -m "$EMAIL"

CERT_DIR="/etc/letsencrypt/live/$DOMAIN"
if [ -d "$CERT_DIR" ]; then
  echo "Obtained certificates in $CERT_DIR"
  echo "Update nginx.conf ssl_certificate paths to point at $CERT_DIR/fullchain.pem and $CERT_DIR/privkey.pem"
else
  echo "Certificate request did not produce expected directory: $CERT_DIR"
  exit 3
fi

echo "Installing cronjob for certbot renew (twice daily)"
sudo bash -c 'cat > /etc/cron.d/certbot_letsencrypt <<EOF
0 0,12 * * * root test -x /usr/bin/certbot && /usr/bin/certbot renew --quiet --post-hook "systemctl reload nginx"
EOF'

echo "Let's Encrypt setup complete. Remember to update nginx.conf ssl_certificate paths and reload nginx."
