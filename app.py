"""imaging-svc — async image transcode + thumbnails."""

from __future__ import annotations

import base64

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from cache import cache_get, cache_put
from db import insert_audit_row
from ssl_config import load_client_ssl
from upstream import charge_credits, publish_transcode_event

app = FastAPI(title="imaging-svc", version="0.4.2")


class TranscodeRequest(BaseModel):
    image_id: str
    payload_b64: str
    target_format: str = "webp"
    quality: int = 80


@app.on_event("startup")
async def _startup() -> None:
    load_client_ssl()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/transcode")
def transcode(req: TranscodeRequest) -> dict[str, str]:
    cached = cache_get(req.image_id)
    if cached is not None:
        return {"image_id": req.image_id, "status": "cache_hit", "size": str(len(cached))}

    try:
        raw = base64.b64decode(req.payload_b64)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"bad b64: {exc}") from exc

    charge_credits(req.image_id, cost_cents=12)
    publish_transcode_event(req.image_id, req.target_format, req.quality)

    transcoded = raw  # placeholder for the real transcode pipeline
    cache_put(req.image_id, transcoded)
    insert_audit_row(req.image_id, req.target_format, len(transcoded))

    return {
        "image_id": req.image_id,
        "status": "ok",
        "bytes": str(len(transcoded)),
    }
