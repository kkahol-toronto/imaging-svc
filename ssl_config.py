"""Outbound mTLS material for talking to billing-rpc."""

from __future__ import annotations

import logging
import ssl
from datetime import datetime, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.backends import default_backend


logger = logging.getLogger(__name__)

CLIENT_CERT_PATH = Path("/etc/imaging-svc/client.crt")
CLIENT_KEY_PATH = Path("/etc/imaging-svc/client.key")

# Surface a warning while there is still time to react. Anything tighter than
# this should also trigger an automated re-issue via cert-manager.
EXPIRY_WARN_THRESHOLD_DAYS = 30


def _check_cert_expiry(cert_path: Path) -> None:
    """Log a clear WARNING if the cert is within the warn window, and an
    ERROR if it is already expired. The previous version had no expiry
    check at all — when the cert lapsed in prod we only learned about it
    via the spike in billing-rpc verification failures."""
    raw = cert_path.read_bytes()
    cert = x509.load_pem_x509_certificate(raw, default_backend())
    expires_at = cert.not_valid_after_utc
    now = datetime.now(timezone.utc)
    days_left = (expires_at - now).days
    if expires_at <= now:
        logger.error(
            "TLS client cert %s EXPIRED on %s (%d days ago)",
            cert_path,
            expires_at.isoformat(),
            -days_left,
        )
    elif days_left <= EXPIRY_WARN_THRESHOLD_DAYS:
        logger.warning(
            "TLS client cert %s expires in %d days (on %s) — rotate ASAP",
            cert_path,
            days_left,
            expires_at.isoformat(),
        )
    else:
        logger.info(
            "TLS client cert %s valid for %d more days",
            cert_path,
            days_left,
        )


def load_client_ssl() -> ssl.SSLContext:
    """Load the outbound client certificate and proactively warn if it is
    expired or close to expiry."""
    _check_cert_expiry(CLIENT_CERT_PATH)
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(
        certfile=str(CLIENT_CERT_PATH),
        keyfile=str(CLIENT_KEY_PATH),
    )
    return ctx
