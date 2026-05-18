# imaging-svc

Async image transcode + thumbnail generation service.

- **Cloud:** AWS · us-east-2
- **Owner:** Imaging
- **Runtime:** Python 3.11 / FastAPI

## Architecture

```
client -> /transcode -> upstream.billing-rpc (charge credits)
                     -> kafka topic image.transcode.requested
                     -> cache (recent results)
                     -> postgres (audit log)
```

## Running locally

```bash
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8080
```

## Files

| Path             | Purpose                                                 |
|------------------|---------------------------------------------------------|
| `app.py`         | FastAPI app + request handlers                          |
| `db.py`          | Postgres connection pool                                |
| `cache.py`       | In-process result cache                                 |
| `upstream.py`    | Outbound calls to `billing-rpc` + Kafka producer        |
| `ssl_config.py`  | TLS material for outbound mTLS                          |

> Service has been degraded in `prod` for several deploys. See incident
> board for the open sev1.
