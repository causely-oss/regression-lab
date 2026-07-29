"""
fraud-detection — checks transactions for fraud. Uses Redis for rate tracking.
Called by billing-service and checkout.
"""

import json, logging, os, random, time
import redis
from fastapi import FastAPI, Request, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fraud-detection")
app = FastAPI(title="fraud-detection")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/9")

REQUEST_COUNT   = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "HTTP request latency", ["method", "endpoint"],
                            buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
FRAUD_CHECKS    = Counter("fraud_checks_total", "Fraud checks", ["risk_level"])

def _redis():
    return redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)

@app.middleware("http")
async def metrics_mw(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    REQUEST_COUNT.labels(request.method, request.url.path, response.status_code).inc()
    REQUEST_LATENCY.labels(request.method, request.url.path).observe(elapsed)
    return response

@app.get("/health")
async def health():
    return {"service": "fraud-detection", "status": "ok"}

@app.post("/fraud/check")
async def check(user_id: str = "user-1", amount: float = 100.0):
    # Track request rate per user
    try:
        r = _redis()
        key = f"fraud:rate:{user_id}"
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, 60, nx=True)
        count, _ = pipe.execute()
        high_rate = count > 20
    except Exception:
        high_rate = False

    # Simple fraud heuristic
    risk = "low"
    if amount > 5000 or high_rate:
        risk = "high"
    elif amount > 1000:
        risk = "medium"

    FRAUD_CHECKS.labels(risk).inc()
    return {"user_id": user_id, "amount": amount, "risk": risk, "approved": risk != "high"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

