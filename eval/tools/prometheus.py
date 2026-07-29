import os
import time
import requests
from typing import Any

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

TOOL_SCHEMAS = [
    {
        "name": "query_prometheus",
        "description": (
            "Execute a PromQL query against Prometheus. "
            "Key metrics: http_requests_total (labels: job, status, endpoint, method), "
            "http_request_duration_seconds_bucket (histogram, label: job), "
            "db_query_duration_seconds_bucket (payments-api only). "
            "Use query_type='instant' for current snapshot, 'range' to see a trend."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "PromQL expression",
                },
                "time_range_minutes": {
                    "type": "integer",
                    "description": "Window for rate() / range queries in minutes (default 5)",
                    "default": 5,
                },
                "query_type": {
                    "type": "string",
                    "enum": ["instant", "range"],
                    "description": "instant = current value, range = values over time_range_minutes (default instant)",
                    "default": "instant",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "find_high_error_rate_services",
        "description": (
            "Return all services whose HTTP 5xx error rate exceeds the threshold. "
            "Good first call to quickly identify unhealthy services."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "threshold_percent": {
                    "type": "number",
                    "description": "Minimum error rate % to include (default 1.0)",
                    "default": 1.0,
                },
                "time_range_minutes": {
                    "type": "integer",
                    "description": "Rate window in minutes (default 5)",
                    "default": 5,
                },
            },
        },
    },
    {
        "name": "find_high_latency_services",
        "description": "Return all services whose p95 HTTP latency exceeds the threshold in milliseconds.",
        "input_schema": {
            "type": "object",
            "properties": {
                "threshold_ms": {
                    "type": "number",
                    "description": "p95 latency threshold in ms (default 100)",
                    "default": 100,
                },
                "time_range_minutes": {
                    "type": "integer",
                    "description": "Rate window in minutes (default 5)",
                    "default": 5,
                },
            },
        },
    },
    {
        "name": "get_service_metrics",
        "description": (
            "Get error rate, p95 latency, and request rate for a specific service. "
            "For payments-api also returns db_query_p95_ms."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Value of the 'job' label (e.g. 'checkout', 'payments-api', 'recommendation-service')",
                },
                "time_range_minutes": {
                    "type": "integer",
                    "description": "Rate window in minutes (default 5)",
                    "default": 5,
                },
            },
            "required": ["service"],
        },
    },
    {
        "name": "list_monitored_services",
        "description": "List all service names (job label values) currently scraped by Prometheus.",
        "input_schema": {"type": "object", "properties": {}},
    },
]


def _instant(query: str) -> list:
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": query, "time": time.time()},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "success":
        raise ValueError(f"Prometheus error: {data.get('error', data)}")
    return data["data"]["result"]


def _range(query: str, minutes: int) -> list:
    end = time.time()
    start = end - minutes * 60
    step = max(15, minutes * 6)
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query_range",
        params={"query": query, "start": start, "end": end, "step": step},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "success":
        raise ValueError(f"Prometheus error: {data.get('error', data)}")
    return data["data"]["result"]


def _fmt_instant(results: list, cap: int = 25) -> list:
    out = []
    for r in results[:cap]:
        labels = {k: v for k, v in r["metric"].items() if k != "__name__"}
        val = r.get("value", [None, None])[1]
        out.append({"labels": labels, "value": _num(val)})
    return out


def _fmt_range(results: list, cap: int = 25) -> list:
    out = []
    for r in results[:cap]:
        labels = {k: v for k, v in r["metric"].items() if k != "__name__"}
        values = r.get("values", [])
        latest = _num(values[-1][1]) if values else None
        out.append({"labels": labels, "latest_value": latest})
    return out


def _num(v) -> float | None:
    if v is None or v == "NaN":
        return None
    try:
        f = float(v)
        return round(f, 4)
    except (ValueError, TypeError):
        return None


def execute(name: str, inputs: dict) -> Any:
    try:
        if name == "query_prometheus":
            return _query_prometheus(**inputs)
        if name == "find_high_error_rate_services":
            return _find_errors(**inputs)
        if name == "find_high_latency_services":
            return _find_latency(**inputs)
        if name == "get_service_metrics":
            return _service_metrics(**inputs)
        if name == "list_monitored_services":
            return _list_services()
        return {"error": f"Unknown tool: {name}"}
    except Exception as e:
        return {"error": str(e)}


def _query_prometheus(query: str, time_range_minutes: int = 5, query_type: str = "instant") -> dict:
    if query_type == "range":
        results = _range(query, time_range_minutes)
        return {"query": query, "type": "range", "results": _fmt_range(results)}
    results = _instant(query)
    return {"query": query, "type": "instant", "results": _fmt_instant(results)}


def _find_errors(threshold_percent: float = 1.0, time_range_minutes: int = 5) -> dict:
    w = f"{time_range_minutes}m"
    q = (
        f"100 * sum by (job) (rate(http_requests_total{{status=~'5..'}}[{w}]))"
        f" / sum by (job) (rate(http_requests_total[{w}])) > {threshold_percent}"
    )
    results = _instant(q)
    services = sorted(
        [{"service": r["metric"].get("job", "?"), "error_rate_pct": round(float(r["value"][1]), 2)} for r in results],
        key=lambda x: x["error_rate_pct"],
        reverse=True,
    )
    return {"threshold_pct": threshold_percent, "window_minutes": time_range_minutes, "services": services}


def _find_latency(threshold_ms: float = 100, time_range_minutes: int = 5) -> dict:
    w = f"{time_range_minutes}m"
    q = (
        f"1000 * histogram_quantile(0.95, sum by (job, le)"
        f" (rate(http_request_duration_seconds_bucket[{w}]))) > {threshold_ms}"
    )
    results = _instant(q)
    services = sorted(
        [{"service": r["metric"].get("job", "?"), "p95_ms": round(float(r["value"][1]), 1)} for r in results],
        key=lambda x: x["p95_ms"],
        reverse=True,
    )
    return {"threshold_ms": threshold_ms, "window_minutes": time_range_minutes, "services": services}


def _service_metrics(service: str, time_range_minutes: int = 5) -> dict:
    w = f"{time_range_minutes}m"

    def scalar(q):
        r = _instant(q)
        return _num(r[0]["value"][1]) if r else None

    err = scalar(
        f"100 * sum(rate(http_requests_total{{job='{service}',status=~'5..'}}[{w}]))"
        f" / sum(rate(http_requests_total{{job='{service}'}}[{w}]))"
    )
    p95 = scalar(
        f"1000 * histogram_quantile(0.95, sum by (le)"
        f" (rate(http_request_duration_seconds_bucket{{job='{service}'}}[{w}])))"
    )
    rps = scalar(f"sum(rate(http_requests_total{{job='{service}'}}[{w}]))")

    result: dict = {
        "service": service,
        "window_minutes": time_range_minutes,
        "error_rate_pct": err,
        "p95_latency_ms": p95,
        "request_rate_rps": rps,
    }

    if service == "payments-api":
        result["db_query_p95_ms"] = scalar(
            f"1000 * histogram_quantile(0.95, sum by (le)"
            f" (rate(db_query_duration_seconds_bucket{{job='{service}'}}[{w}])))"
        )

    return result


def _list_services() -> dict:
    results = _instant("group by (job) (up)")
    jobs = sorted(r["metric"]["job"] for r in results if "job" in r["metric"])
    return {"services": jobs, "count": len(jobs)}
