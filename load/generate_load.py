#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Load generator for Scenario 01 — Checkout Failure from Upstream DB

Generates traffic across all major application flows to drive all 36 services:
  - Checkout:  POST /checkout  (-> pricing -> discount -> loyalty -> user-service;
                                 -> billing -> payment-adapter -> external-payment-api,
                                    tax-service, fraud-detection;
                                 -> payments-api -> Kafka -> audit-service;
                                 -> fraud-detection)
  - Browse:    GET  /browse    (local, fast)
  - Search:    GET  /search    (-> ranking -> profile -> user-service, media-service;
                                 -> cache-service)
  - Auth:      POST /auth/login (-> session -> user-service; -> Kafka -> audit-service)
  - Catalog:   GET  /catalog/product/{id} (deep chain: catalog -> review -> recs ->
                 analytics -> reporting; recs -> delivery -> notification -> email;
                 catalog -> media-service, cache-service)
  - Orders:    GET  /orders    (-> orders-service -> inventory -> warehouse ->
                 Kafka -> notification, shipping -> warehouse, notification)
  - Cart:      GET  /cart/{user_id}  &  POST /cart/{user_id}/add
               (-> cart-service -> catalog-service)
  - Ingest:    POST /ingest    (-> ingest-service -> Kafka -> processing-service ->
                 Kafka -> recommendation-service)

Usage:
  python generate_load.py [--frontend-url URL] [--duration SECONDS]

Options:
  --frontend-url  Base URL of the frontend service  (default: http://localhost:8080)
  --checkout-rps  Checkout requests per second       (default: 5)
  --browse-rps    Browse requests per second         (default: 100)
  --search-rps    Search requests per second         (default: 20)
  --auth-rps      Auth requests per second           (default: 3)
  --catalog-rps   Catalog (deep chain) req/sec       (default: 8)
  --orders-rps    Orders requests per second         (default: 5)
  --cart-rps      Cart requests per second            (default: 10)
  --ingest-rps    Ingest requests per second          (default: 3)
  --duration      Run for N seconds then exit        (default: run until Ctrl+C)
  --report-every  Print stats summary every N sec    (default: 10)
"""

import argparse
import asyncio
import random
import signal
import sys
import time
from dataclasses import dataclass, field

import aiohttp

# -- Config --------------------------------------------------------------------

DEFAULT_FRONTEND_URL = "http://localhost:8080"
DEFAULT_CHECKOUT_RPS = 5
DEFAULT_BROWSE_RPS   = 100
DEFAULT_SEARCH_RPS   = 20
DEFAULT_AUTH_RPS     = 3
DEFAULT_CATALOG_RPS  = 8
DEFAULT_ORDERS_RPS   = 5
DEFAULT_CART_RPS     = 10
DEFAULT_INGEST_RPS   = 3
DEFAULT_REPORT_EVERY = 10


# -- Stats ---------------------------------------------------------------------

@dataclass
class Stats:
    requests:    int = 0
    successes:   int = 0
    errors:      int = 0
    timeouts:    int = 0
    latencies_ms: list = field(default_factory=list)
    start_time:  float = field(default_factory=time.perf_counter)

    def record(self, status: int, latency_ms: float):
        self.requests += 1
        if 200 <= status < 300:
            self.successes += 1
        else:
            self.errors += 1
        self.latencies_ms.append(latency_ms)

    def record_timeout(self):
        self.requests  += 1
        self.timeouts  += 1
        self.errors    += 1

    def success_rate(self) -> float:
        if self.requests == 0:
            return 100.0
        return 100.0 * self.successes / self.requests

    def p95_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * 0.95)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    def reset_window(self):
        self.requests     = 0
        self.successes    = 0
        self.errors       = 0
        self.timeouts     = 0
        self.latencies_ms = []
        self.start_time   = time.perf_counter()


# Per-workload stats
workload_stats = {
    "checkout": Stats(),
    "browse":   Stats(),
    "search":   Stats(),
    "auth":     Stats(),
    "catalog":  Stats(),
    "orders":   Stats(),
    "cart":     Stats(),
    "ingest":   Stats(),
}
running = True


# -- Workers -------------------------------------------------------------------

async def do_checkout(session: aiohttp.ClientSession, base_url: str):
    user_id = f"user-{random.randint(1, 100)}"
    url = f"{base_url}/checkout"
    start = time.perf_counter()
    try:
        async with session.post(url, params={"user_id": user_id},
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["checkout"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["checkout"].record_timeout()


async def do_browse(session: aiohttp.ClientSession, base_url: str):
    page = random.randint(1, 5)
    url = f"{base_url}/browse"
    start = time.perf_counter()
    try:
        async with session.get(url, params={"page": page},
                               timeout=aiohttp.ClientTimeout(total=3)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["browse"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["browse"].record_timeout()


async def do_search(session: aiohttp.ClientSession, base_url: str):
    queries = ["laptop", "phone", "headphones", "shirt", "shoes", "book", "camera", "tablet"]
    q = random.choice(queries)
    url = f"{base_url}/search"
    start = time.perf_counter()
    try:
        async with session.get(url, params={"q": q, "page": 1},
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["search"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["search"].record_timeout()


async def do_auth(session: aiohttp.ClientSession, base_url: str):
    user_id = f"user-{random.randint(1, 50)}"
    url = f"{base_url}/auth/login"
    start = time.perf_counter()
    try:
        async with session.post(url, params={"user_id": user_id, "password": "pass"},
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["auth"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["auth"].record_timeout()


async def do_catalog(session: aiohttp.ClientSession, base_url: str):
    """Deep chain: frontend -> gateway -> catalog -> review -> recommendation -> analytics -> reporting"""
    product_id = f"prod-{random.randint(1, 50)}"
    url = f"{base_url}/catalog/product/{product_id}"
    start = time.perf_counter()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["catalog"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["catalog"].record_timeout()


async def do_orders(session: aiohttp.ClientSession, base_url: str):
    user_id = f"user-{random.randint(1, 50)}"
    url = f"{base_url}/orders"
    start = time.perf_counter()
    try:
        async with session.get(url, params={"user_id": user_id},
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["orders"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["orders"].record_timeout()


async def do_cart(session: aiohttp.ClientSession, base_url: str):
    """Cart operations: GET /cart/{user_id} (60%) or POST /cart/{user_id}/add (40%).
    Drives: frontend -> api-gateway -> cart-service -> catalog-service"""
    user_id = f"user-{random.randint(1, 50)}"
    start = time.perf_counter()
    try:
        if random.random() < 0.6:
            # Read cart
            url = f"{base_url}/cart/{user_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.read()
                latency = (time.perf_counter() - start) * 1000
                workload_stats["cart"].record(resp.status, latency)
        else:
            # Add item to cart
            product_id = f"prod-{random.randint(1, 50)}"
            url = f"{base_url}/cart/{user_id}/add"
            async with session.post(url, params={"product_id": product_id},
                                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.read()
                latency = (time.perf_counter() - start) * 1000
                workload_stats["cart"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["cart"].record_timeout()


async def do_ingest(session: aiohttp.ClientSession, base_url: str):
    """Ingest events: POST /ingest with varying event types.
    Drives: frontend -> ingest-service -> Kafka -> processing-service ->
            Kafka -> recommendation-service"""
    event_types = ["click", "view", "purchase", "scroll"]
    event_type = random.choice(event_types)
    user_id = f"user-{random.randint(1, 100)}"
    url = f"{base_url}/ingest"
    start = time.perf_counter()
    try:
        async with session.post(url, params={"event_type": event_type, "user_id": user_id},
                                timeout=aiohttp.ClientTimeout(total=5)) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            workload_stats["ingest"].record(resp.status, latency)
    except (asyncio.TimeoutError, Exception):
        workload_stats["ingest"].record_timeout()


async def rate_limited_worker(fn, session, base_url, rps, label):
    """Fire `fn` at the given RPS using a token-bucket approach."""
    interval = 1.0 / rps
    while running:
        asyncio.ensure_future(fn(session, base_url))
        await asyncio.sleep(interval)


async def reporter(interval: int):
    """Print a periodic summary to stdout."""
    global workload_stats
    await asyncio.sleep(interval)
    while running:
        c   = workload_stats["checkout"]
        b   = workload_stats["browse"]
        s   = workload_stats["search"]
        a   = workload_stats["auth"]
        cat = workload_stats["catalog"]
        o   = workload_stats["orders"]
        cr  = workload_stats["cart"]
        ing = workload_stats["ingest"]

        total_req = sum(ws.requests for ws in workload_stats.values())
        total_err = sum(ws.errors for ws in workload_stats.values())
        total_rps = total_req / interval

        def fmt(label, st, show_details=True):
            if show_details:
                pct = f"{st.success_rate():.0f}%" if st.requests > 0 else "- "
                p95 = f"{st.p95_ms():.0f}ms" if st.latencies_ms else "-"
                return f"{label}={st.requests:>4} ({pct:>4} {p95:>6})"
            return f"{label}={st.requests:>4}"

        print(
            f"[{time.strftime('%H:%M:%S')}] "
            f"{fmt('chk', c)} | "
            f"{fmt('brw', b, False)} | "
            f"{fmt('srch', s)} | "
            f"{fmt('auth', a)} | "
            f"{fmt('cat', cat)} | "
            f"{fmt('ord', o)} | "
            f"{fmt('cart', cr)} | "
            f"{fmt('ing', ing)} | "
            f"rps={total_rps:>4.0f} err={total_err}"
        )

        # Reset window counters
        workload_stats = {k: Stats() for k in workload_stats}
        await asyncio.sleep(interval)


# -- Main ----------------------------------------------------------------------

async def main(args):
    global running

    connector = aiohttp.TCPConnector(limit=512, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        total_rps = (args.checkout_rps + args.browse_rps + args.search_rps +
                     args.auth_rps + args.catalog_rps + args.orders_rps +
                     args.cart_rps + args.ingest_rps)
        print(f"Starting load against {args.frontend_url}")
        print(f"  Checkout:  {args.checkout_rps} req/sec  (~{args.checkout_rps * 60:.0f}/min)")
        print(f"  Browse:    {args.browse_rps} req/sec")
        print(f"  Search:    {args.search_rps} req/sec")
        print(f"  Auth:      {args.auth_rps} req/sec")
        print(f"  Catalog:   {args.catalog_rps} req/sec  (deep chain)")
        print(f"  Orders:    {args.orders_rps} req/sec")
        print(f"  Cart:      {args.cart_rps} req/sec  (60% GET / 40% POST)")
        print(f"  Ingest:    {args.ingest_rps} req/sec  (event pipeline)")
        print(f"  Total:     ~{total_rps:.0f} req/sec")
        print(f"  Report:    every {args.report_every}s")
        if args.duration:
            print(f"  Duration:  {args.duration}s")
        print("Press Ctrl+C to stop.\n")

        tasks = [
            asyncio.create_task(
                rate_limited_worker(do_checkout, session, args.frontend_url, args.checkout_rps, "checkout")),
            asyncio.create_task(
                rate_limited_worker(do_browse, session, args.frontend_url, args.browse_rps, "browse")),
            asyncio.create_task(
                rate_limited_worker(do_search, session, args.frontend_url, args.search_rps, "search")),
            asyncio.create_task(
                rate_limited_worker(do_auth, session, args.frontend_url, args.auth_rps, "auth")),
            asyncio.create_task(
                rate_limited_worker(do_catalog, session, args.frontend_url, args.catalog_rps, "catalog")),
            asyncio.create_task(
                rate_limited_worker(do_orders, session, args.frontend_url, args.orders_rps, "orders")),
            asyncio.create_task(
                rate_limited_worker(do_cart, session, args.frontend_url, args.cart_rps, "cart")),
            asyncio.create_task(
                rate_limited_worker(do_ingest, session, args.frontend_url, args.ingest_rps, "ingest")),
            asyncio.create_task(reporter(args.report_every)),
        ]

        if args.duration:
            await asyncio.sleep(args.duration)
            running = False
            for t in tasks:
                t.cancel()
        else:
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                pass


def handle_signal(sig, frame):
    global running
    print("\nShutting down load generator...")
    running = False
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--frontend-url",  default=DEFAULT_FRONTEND_URL)
    parser.add_argument("--checkout-rps",  type=float, default=DEFAULT_CHECKOUT_RPS)
    parser.add_argument("--browse-rps",    type=float, default=DEFAULT_BROWSE_RPS)
    parser.add_argument("--search-rps",    type=float, default=DEFAULT_SEARCH_RPS)
    parser.add_argument("--auth-rps",      type=float, default=DEFAULT_AUTH_RPS)
    parser.add_argument("--catalog-rps",   type=float, default=DEFAULT_CATALOG_RPS)
    parser.add_argument("--orders-rps",    type=float, default=DEFAULT_ORDERS_RPS)
    parser.add_argument("--cart-rps",      type=float, default=DEFAULT_CART_RPS)
    parser.add_argument("--ingest-rps",    type=float, default=DEFAULT_INGEST_RPS)
    parser.add_argument("--duration",      type=int,   default=None, help="Stop after N seconds")
    parser.add_argument("--report-every",  type=int,   default=DEFAULT_REPORT_EVERY)
    args = parser.parse_args()

    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(main(args))
