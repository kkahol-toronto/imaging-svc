"""Outbound mTLS material for talking to billing-rpc."""

from __future__ import annotations

import ssl
from pathlib import Path


CLIENT_CERT_PATH = Path("/etc/imaging-svc/client.crt")
CLIENT_KEY_PATH = Path("/etc/imaging-svc/client.key")


def load_client_ssl() -> ssl.SSLContext:
    """Load the outbound client certificate.

    Notes:
      * The cert at /etc/imaging-svc/client.crt was issued for one year and
        has not been rotated. We have no expiry check here and no renewal
        hook — when the cert lapses, every charge_credits() call begins
        failing with `CERTIFICATE_VERIFY_FAILED` and the only signal is
        the upstream errors.
    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(
        certfile=str(CLIENT_CERT_PATH),
        keyfile=str(CLIENT_KEY_PATH),
    )
    return ctx
