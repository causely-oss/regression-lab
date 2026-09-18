#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
generate_dashboard.py
Generates the Grafana dashboard JSON for scenario-01 and writes it to:
  - environment/grafana-dashboard.json        (docker-compose volume)
  - k8s/01-configmaps.yaml                   (k8s ConfigMap, inline)
"""

import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))

# ── Panel builders ──────────────────────────────────────────────────────────

_id = 0
def next_id():
    global _id
    _id += 1
    return _id

def stat(title, expr, unit="short", thresholds=None, x=0, y=0, w=4, h=4):
    if thresholds is None:
        thresholds = [{"color": "green", "value": None}]
    return {
        "id": next_id(),
        "title": title,
        "type": "stat",
        "datasource": "Prometheus",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": thresholds},
            }
        },
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "orientation": "auto", "textMode": "auto", "colorMode": "background"},
        "targets": [{"expr": expr, "legendFormat": title, "refId": "A"}],
    }

def timeseries(title, targets, unit="short", x=0, y=0, w=12, h=8):
    return {
        "id": next_id(),
        "title": title,
        "type": "timeseries",
        "datasource": "Prometheus",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 5}},
        },
        "options": {"tooltip": {"mode": "multi"}, "legend": {"displayMode": "table", "placement": "bottom"}},
        "targets": [{"expr": e, "legendFormat": lf, "refId": chr(65 + i)} for i, (e, lf) in enumerate(targets)],
    }

def row_panel(title, y=0):
    return {
        "id": next_id(),
        "title": title,
        "type": "row",
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        "collapsed": False,
    }

def rps(job):
    return f'sum(rate(http_requests_total{{job="{job}"}}[1m]))'

def err_rate(job):
    return (f'100 * sum(rate(http_requests_total{{job="{job}",status=~"5.."}}[1m])) '
            f'/ sum(rate(http_requests_total{{job="{job}"}}[1m]))')

def p95(job):
    return (f'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{job="{job}"}}[1m])) by (le)) * 1000')

def p50(job):
    return (f'histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{{job="{job}"}}[1m])) by (le)) * 1000')

# ── Build panels ─────────────────────────────────────────────────────────────

panels = []
y = 0

# ════════════════════════════════════════════════════════════════
# ROW 0 — Platform Health (stat overview)
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("🏥 Platform Health", y=y)); y += 1

panels.append(stat(
    "Checkout Success Rate",
    '100 * sum(rate(http_requests_total{job="checkout",status=~"2.."}[1m])) / sum(rate(http_requests_total{job="checkout"}[1m]))',
    unit="percent", x=0, y=y, w=4, h=4,
    thresholds=[{"color": "red", "value": None}, {"color": "yellow", "value": 95}, {"color": "green", "value": 99}]
))
panels.append(stat(
    "Platform Error Rate",
    '100 * sum(rate(http_requests_total{status=~"5.."}[1m])) / sum(rate(http_requests_total[1m]))',
    unit="percent", x=4, y=y, w=4, h=4,
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}, {"color": "red", "value": 5}]
))
panels.append(stat(
    "payments-api p95 Latency",
    p95("payments-api"),
    unit="ms", x=8, y=y, w=4, h=4,
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 500}, {"color": "red", "value": 1000}]
))
panels.append(stat(
    "DB Query Duration (avg ms)",
    'pg_stat_activity_max_tx_duration{datname="payments"} * 1000',
    unit="ms", x=12, y=y, w=4, h=4,
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 200}, {"color": "red", "value": 1000}]
))
panels.append(stat(
    "DB Active Connections",
    'pg_stat_database_numbackends{datname="payments"}',
    unit="short", x=16, y=y, w=4, h=4,
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 70}, {"color": "red", "value": 85}]
))
panels.append(stat(
    "Redis Memory Used",
    'redis_memory_used_bytes / 1024 / 1024',
    unit="mbytes", x=20, y=y, w=4, h=4,
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 400}, {"color": "red", "value": 480}]
))
y += 4

# ════════════════════════════════════════════════════════════════
# ROW 1 — Total Request Rate (all services)
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("📊 Request Rate — All Services", y=y)); y += 1

ALL_APP_SERVICES = [
    "frontend", "api-gateway", "checkout", "payments-api",
    "auth-service", "orders-service", "inventory-service", "shipping-service",
    "search-service", "ranking-service", "profile-service",
    "billing-service", "payment-adapter", "ingest-service", "processing-service",
    "recommendation-service", "delivery-service", "pricing-service", "discount-service",
    "user-service", "notification-service", "cart-service", "catalog-service",
    "review-service", "loyalty-service", "audit-service", "session-service",
    "analytics-service", "email-service", "cache-service", "reporting-service",
    "fraud-detection", "tax-service", "warehouse-service", "media-service",
    "external-payment-api",
]

panels.append(timeseries(
    "Request Rate — All Services (req/s)",
    [(rps(svc), svc) for svc in ALL_APP_SERVICES],
    unit="reqps", x=0, y=y, w=24, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 2 — Error Rates by Flow
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("🔴 Error Rates by Flow", y=y)); y += 1

panels.append(timeseries(
    "Error Rate — Checkout Pipeline (%)",
    [
        (err_rate("frontend"),        "frontend"),
        (err_rate("api-gateway"),     "api-gateway"),
        (err_rate("checkout"),        "checkout"),
        (err_rate("payments-api"),    "payments-api"),
        (err_rate("billing-service"), "billing-service"),
        (err_rate("payment-adapter"), "payment-adapter"),
        (err_rate("pricing-service"), "pricing-service"),
        (err_rate("fraud-detection"), "fraud-detection"),
    ],
    unit="percent", x=0, y=y, w=12, h=8
))
panels.append(timeseries(
    "Error Rate — Search, Catalog & Auth (%)",
    [
        (err_rate("search-service"),        "search-service"),
        (err_rate("ranking-service"),       "ranking-service"),
        (err_rate("catalog-service"),       "catalog-service"),
        (err_rate("review-service"),        "review-service"),
        (err_rate("recommendation-service"),"recommendation-service"),
        (err_rate("analytics-service"),     "analytics-service"),
        (err_rate("reporting-service"),     "reporting-service"),
        (err_rate("auth-service"),          "auth-service"),
        (err_rate("session-service"),       "session-service"),
    ],
    unit="percent", x=12, y=y, w=12, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 3 — Latency by Flow
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("⏱ Latency p95 by Flow", y=y)); y += 1

panels.append(timeseries(
    "p95 Latency — Checkout Flow (ms)",
    [
        (p95("checkout"),          "checkout"),
        (p95("payments-api"),      "payments-api"),
        (p95("billing-service"),   "billing-service"),
        (p95("payment-adapter"),   "payment-adapter"),
        (p95("pricing-service"),   "pricing-service"),
        (p95("discount-service"),  "discount-service"),
        (p95("fraud-detection"),   "fraud-detection"),
        (p95("tax-service"),       "tax-service"),
    ],
    unit="ms", x=0, y=y, w=12, h=8
))
panels.append(timeseries(
    "p95 Latency — Deep Chain: Catalog→Review→Rec→Analytics→Reporting (ms)",
    [
        (p95("api-gateway"),               "api-gateway"),
        (p95("catalog-service"),           "catalog-service"),
        (p95("review-service"),            "review-service"),
        (p95("recommendation-service"),    "recommendation-service"),
        (p95("analytics-service"),         "analytics-service"),
        (p95("reporting-service"),         "reporting-service"),
    ],
    unit="ms", x=12, y=y, w=12, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 4 — Auth, Search & Orders Flows
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("🔐 Auth · 🔍 Search · 📦 Orders Flows", y=y)); y += 1

panels.append(timeseries(
    "Auth Flow — Request Rate (req/s)",
    [
        (rps("auth-service"),    "auth-service"),
        (rps("session-service"), "session-service"),
        (rps("user-service"),    "user-service"),
    ],
    unit="reqps", x=0, y=y, w=8, h=8
))
panels.append(timeseries(
    "Search Flow — Request Rate (req/s)",
    [
        (rps("search-service"),  "search-service"),
        (rps("ranking-service"), "ranking-service"),
        (rps("profile-service"), "profile-service"),
        (rps("user-service"),    "user-service"),
        (rps("cache-service"),   "cache-service"),
    ],
    unit="reqps", x=8, y=y, w=8, h=8
))
panels.append(timeseries(
    "Order Pipeline — Request Rate (req/s)",
    [
        (rps("orders-service"),    "orders-service"),
        (rps("inventory-service"), "inventory-service"),
        (rps("shipping-service"),  "shipping-service"),
        (rps("warehouse-service"), "warehouse-service"),
    ],
    unit="reqps", x=16, y=y, w=8, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 5 — Billing, Streaming & Support
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("💳 Billing · 📡 Streaming · 🔔 Support Services", y=y)); y += 1

panels.append(timeseries(
    "Billing Flow — Request Rate (req/s)",
    [
        (rps("billing-service"),       "billing-service"),
        (rps("payment-adapter"),       "payment-adapter"),
        (rps("external-payment-api"),  "external-payment-api"),
        (rps("tax-service"),           "tax-service"),
        (rps("fraud-detection"),       "fraud-detection"),
    ],
    unit="reqps", x=0, y=y, w=8, h=8
))
panels.append(timeseries(
    "Streaming Flow — Request Rate (req/s)",
    [
        (rps("ingest-service"),          "ingest-service"),
        (rps("processing-service"),      "processing-service"),
        (rps("recommendation-service"),  "recommendation-service"),
        (rps("delivery-service"),        "delivery-service"),
    ],
    unit="reqps", x=8, y=y, w=8, h=8
))
panels.append(timeseries(
    "Support Services — Request Rate (req/s)",
    [
        (rps("notification-service"), "notification-service"),
        (rps("email-service"),        "email-service"),
        (rps("audit-service"),        "audit-service"),
        (rps("loyalty-service"),      "loyalty-service"),
        (rps("cart-service"),         "cart-service"),
        (rps("media-service"),        "media-service"),
    ],
    unit="reqps", x=16, y=y, w=8, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 6 — DB, Redis & Infrastructure
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("🗄 Database · Redis · Infrastructure", y=y)); y += 1

panels.append(timeseries(
    "DB Query Duration (ms)",
    [
        ('rate(db_query_duration_seconds_sum{job="payments-api"}[1m]) / rate(db_query_duration_seconds_count{job="payments-api"}[1m]) * 1000',
         "avg query ms"),
        (p95("payments-api"), "payments-api p95"),
        ('histogram_quantile(0.99, sum(rate(db_query_duration_seconds_bucket{job="payments-api"}[1m])) by (le)) * 1000',
         "p99 query ms"),
    ],
    unit="ms", x=0, y=y, w=8, h=8
))
panels.append(timeseries(
    "DB Connections & Pool",
    [
        ('pg_stat_database_numbackends{datname="payments"}', "active connections"),
        ('db_pool_connections{job="payments-api"}',          "pool size"),
    ],
    unit="short", x=8, y=y, w=8, h=8
))
panels.append(timeseries(
    "Redis",
    [
        ('redis_connected_clients',             "connected clients"),
        ('redis_memory_used_bytes / 1024 / 1024', "memory MB"),
        ('rate(redis_commands_processed_total[1m])', "commands/s"),
    ],
    unit="short", x=16, y=y, w=8, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 7 — Latency Percentile Detail (p50 / p95 / p99)
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("📈 Latency Percentiles — Checkout & payments-api", y=y)); y += 1

panels.append(timeseries(
    "Checkout Latency Percentiles (ms)",
    [
        (p50("checkout"),  "checkout p50"),
        (p95("checkout"),  "checkout p95"),
        (f'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{{job="checkout"}}[1m])) by (le)) * 1000',
         "checkout p99"),
    ],
    unit="ms", x=0, y=y, w=12, h=8
))
panels.append(timeseries(
    "payments-api Latency Percentiles (ms)",
    [
        (p50("payments-api"),  "payments-api p50"),
        (p95("payments-api"),  "payments-api p95"),
        (f'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{{job="payments-api"}}[1m])) by (le)) * 1000',
         "payments-api p99"),
    ],
    unit="ms", x=12, y=y, w=12, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 8 — Full Error Rate Heatmap (all services)
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("🌡 Error Rate — All Services", y=y)); y += 1

panels.append(timeseries(
    "Error Rate — All Services (%)",
    [(err_rate(svc), svc) for svc in ALL_APP_SERVICES],
    unit="percent", x=0, y=y, w=24, h=8
))
y += 8

# ════════════════════════════════════════════════════════════════
# ROW 9 — p95 Latency All Services
# ════════════════════════════════════════════════════════════════
panels.append(row_panel("⏱ p95 Latency — All Services", y=y)); y += 1

panels.append(timeseries(
    "p95 Latency — All Services (ms)",
    [(p95(svc), svc) for svc in ALL_APP_SERVICES],
    unit="ms", x=0, y=y, w=24, h=8
))

# ── Assemble dashboard ───────────────────────────────────────────────────────

dashboard = {
    "__inputs": [],
    "__requires": [],
    "annotations": {"list": []},
    "editable": True,
    "fiscalYearStartMonth": 0,
    "graphTooltip": 1,
    "id": None,
    "links": [],
    "panels": panels,
    "refresh": "10s",
    "schemaVersion": 38,
    "tags": ["oncall-eval", "scenario-01"],
    "time": {"from": "now-30m", "to": "now"},
    "timepicker": {},
    "timezone": "browser",
    "title": "Scenario 01 — Full Platform (36 services)",
    "uid": "scenario-01-checkout",
    "version": 2,
}

dashboard_json = json.dumps(dashboard, indent=2)

# ── Write standalone file (docker-compose) ───────────────────────────────────

out_compose = os.path.join(BASE, "environment", "grafana-dashboard.json")
with open(out_compose, "w") as f:
    f.write(dashboard_json)
print(f"Written: {out_compose}")
print(f"  Panels: {len(panels)}")

# ── Patch k8s/01-configmaps.yaml ────────────────────────────────────────────

cm_path = os.path.join(BASE, "k8s", "01-configmaps.yaml")
with open(cm_path, "r") as f:
    cm_content = f.read()

# Indent every line of the JSON by 4 spaces (for the ConfigMap data block)
indented_json = "\n".join("    " + line for line in dashboard_json.splitlines())
new_block = f"  scenario-01.json: |\n{indented_json}\n"

# Replace everything between "scenario-01.json: |" and the next "---" or end of file
cm_content = re.sub(
    r"  scenario-01\.json: \|.*?(?=\n---|\Z)",
    lambda _: new_block.rstrip(),
    cm_content,
    flags=re.DOTALL,
)

with open(cm_path, "w") as f:
    f.write(cm_content)
print(f"Patched:  {cm_path}")
print(f"\nDone. Dashboard has {len(panels)} panels across 10 sections.")
