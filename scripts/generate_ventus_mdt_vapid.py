#!/usr/bin/env python3
"""
Generate VAPID keys for Ventus MDT Web Push (Sparrow ERP ventus_response_module).

Dependencies: pywebpush / py-vapid (install with: pip install -r requirements.txt)

Usage (from repository root):
    python scripts/generate_ventus_mdt_vapid.py
    python scripts/generate_ventus_mdt_vapid.py --subject mailto:ops@example.com

Alternative (Node.js): npx -y web-push generate-vapid-keys

Add the printed variables to app/config/.env and restart the application.
Serve the MDT over HTTPS so browsers allow push subscription.
"""
from __future__ import annotations

import argparse
import sys

from cryptography.hazmat.primitives import serialization


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--subject",
        default="mailto:mdt@localhost",
        help="Suggested VENTUS_MDT_VAPID_SUBJECT (mailto: or https: URL)",
    )
    args = parser.parse_args()

    try:
        from py_vapid import Vapid01
        from py_vapid.utils import b64urlencode
    except ImportError:
        print(
            "Missing py_vapid. Install dependencies: pip install -r requirements.txt",
            file=sys.stderr,
        )
        return 1

    v = Vapid01()
    v.generate_keys()

    raw_pub = v.public_key.public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint,
    )
    pub_b64 = b64urlencode(raw_pub)
    if isinstance(pub_b64, bytes):
        pub_b64 = pub_b64.decode("ascii")

    der = v.private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    priv_b64 = b64urlencode(der)
    if isinstance(priv_b64, bytes):
        priv_b64 = priv_b64.decode("ascii")

    subj = (args.subject or "").strip()
    if not subj.startswith("mailto:") and not subj.startswith("https:"):
        subj = "mailto:mdt@localhost"

    print("# Paste into app/config/.env (then restart Sparrow ERP)")
    print(f"VENTUS_MDT_VAPID_PUBLIC_KEY={pub_b64}")
    print(f"VENTUS_MDT_VAPID_PRIVATE_KEY={priv_b64}")
    print(f"VENTUS_MDT_VAPID_SUBJECT={subj}")
    print()
    print("# Optional: PEM private key (multiline .env). One-line escape for Docker/systemd:")
    pem = v.private_pem().decode("utf-8").strip()
    pem_escaped = pem.replace("\n", "\\n")
    print(f"# VENTUS_MDT_VAPID_PRIVATE_KEY={pem_escaped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
