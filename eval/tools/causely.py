"""
Causely API tool implementations.

Requires environment variables:
  CAUSELY_API_URL  — base URL, e.g. https://app.causely.io
  CAUSELY_API_KEY  — bearer token

Expected REST endpoints (adjust to match your deployment):
  GET /api/v1/health                          — platform health summary
  GET /api/v1/symptoms[?service=<name>]       — active symptoms
  GET /api/v1/root-causes[?service=<name>]    — root cause analysis
  GET /api/v1/services/<name>/health          — single service health
  GET /api/v1/topology[?service=<name>]       — dependency graph
"""

import os
import requests
from typing import Any, Optional

CAUSELY_API_URL = os.environ.get("CAUSELY_API_URL", "")
CAUSELY_API_KEY = os.environ.get("CAUSELY_API_KEY", "")

TOOL_SCHEMAS = [
    {
        "name": "get_platform_health",
        "description": (
            "Get Causely's overall platform health summary: active symptom count, "
            "affected service count, and top issues. Best first call to quickly assess state."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_active_symptoms",
        "description": (
            "List all active symptoms (anomalies) Causely has detected. "
            "Each symptom includes affected service, type (ErrorRate, Latency, etc.), "
            "severity, and start time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Optional: filter to symptoms affecting this service",
                }
            },
        },
    },
    {
        "name": "get_root_cause_analysis",
        "description": (
            "Get Causely's root cause analysis. Causely automatically correlates symptoms "
            "across services to distinguish root causes from victims."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Optional: scope the analysis to symptoms affecting this service",
                }
            },
        },
    },
    {
        "name": "get_service_health",
        "description": (
            "Get Causely health details for a specific service: active symptoms, "
            "SLO status, and whether it is a root cause or a downstream victim."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Service name (e.g. 'checkout', 'payments-api', 'recommendation-service')",
                }
            },
            "required": ["service"],
        },
    },
    {
        "name": "get_service_topology",
        "description": (
            "Get service dependency topology from Causely showing upstream callers "
            "and downstream dependencies. Useful for tracing blast radius."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Optional: center the view on this service",
                }
            },
        },
    },
]


def _get(path: str, params: Optional[dict] = None) -> dict:
    if not CAUSELY_API_URL:
        raise ValueError("CAUSELY_API_URL is not set. Export it before running the eval.")
    headers = {"Accept": "application/json"}
    if CAUSELY_API_KEY:
        headers["Authorization"] = f"Bearer {CAUSELY_API_KEY}"
    resp = requests.get(
        f"{CAUSELY_API_URL.rstrip('/')}/{path.lstrip('/')}",
        params={k: v for k, v in (params or {}).items() if v is not None},
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def execute(name: str, inputs: dict) -> Any:
    try:
        if name == "get_platform_health":
            return _get("/api/v1/health")
        if name == "list_active_symptoms":
            return _get("/api/v1/symptoms", {"service": inputs.get("service")})
        if name == "get_root_cause_analysis":
            return _get("/api/v1/root-causes", {"service": inputs.get("service")})
        if name == "get_service_health":
            return _get(f"/api/v1/services/{inputs['service']}/health")
        if name == "get_service_topology":
            return _get("/api/v1/topology", {"service": inputs.get("service")})
        return {"error": f"Unknown Causely tool: {name}"}
    except Exception as e:
        return {"error": str(e)}
