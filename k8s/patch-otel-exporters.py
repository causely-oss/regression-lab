#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Patch all scenario-01 Go services to export OTLP via WithEndpoint (Alloy /v1/traces)."""
from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent / "environment" / "services"

HELPERS = '''
func otlpHTTPHostPort(raw string) string {
	u := strings.TrimSpace(raw)
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	if i := strings.Index(u, "/"); i >= 0 {
		u = u[:i]
	}
	return strings.TrimSuffix(u, "/")
}

func annotateHTTPSpan(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r)
		if span := oteltrace.SpanFromContext(r.Context()); span.IsRecording() {
			span.SetAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.route", r.URL.Path),
				attribute.String("http.target", r.URL.Path),
				attribute.Int("http.status_code", rec.statusCode),
			)
		}
	})
}

func spanNameFromRequest(_ string, r *http.Request) string {
	return r.Method + " " + r.URL.Path
}

'''

OLD_OPTS = """	ctx := context.Background()
	opts := []otlptracehttp.Option{otlptracehttp.WithInsecure()}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		opts = append(opts, otlptracehttp.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
	}"""

NEW_OPTS = """	ctx := context.Background()
	opts := []otlptracehttp.Option{
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(otlpHTTPHostPort(endpoint)),
	}"""

IMPORT_ATTR = '\t"go.opentelemetry.io/otel/attribute"\n'
IMPORT_TRACE = '\toteltrace "go.opentelemetry.io/otel/trace"\n'


def ensure_imports(src: str) -> str:
    if '"go.opentelemetry.io/otel/attribute"' not in src:
        src = src.replace(
            '\t"go.opentelemetry.io/otel"\n',
            '\t"go.opentelemetry.io/otel"\n' + IMPORT_ATTR,
        )
    if 'oteltrace "go.opentelemetry.io/otel/trace"' not in src:
        src = src.replace(
            '\t"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"\n',
            '\t"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"\n' + IMPORT_TRACE,
        )
    if '"strings"' not in src:
        src = src.replace('\t"os"\n', '\t"os"\n\t"strings"\n')
    return src


def insert_helpers(src: str) -> str:
    if "func otlpHTTPHostPort(" in src:
        return src
    # Insert helpers immediately before initTracer
    return src.replace("func initTracer() func() {", HELPERS + "func initTracer() func() {", 1)


def wrap_handler(src: str) -> str:
    if "annotateHTTPSpan(" in src:
        return src
    # Common: otelhttp.NewHandler(mux, serviceName, otelhttp.WithFilter(...)
    src = src.replace(
        "otelhttp.NewHandler(mux, serviceName, otelhttp.WithFilter(",
        "otelhttp.NewHandler(annotateHTTPSpan(mux), serviceName, otelhttp.WithSpanNameFormatter(spanNameFromRequest), otelhttp.WithFilter(",
    )
    src = src.replace(
        'otelhttp.NewHandler(metricsMiddleware(mux), "payments-api", otelhttp.WithFilter(',
        'otelhttp.NewHandler(annotateHTTPSpan(metricsMiddleware(mux)), "payments-api", otelhttp.WithSpanNameFormatter(spanNameFromRequest), otelhttp.WithFilter(',
    )
    src = src.replace(
        'otelhttp.NewHandler(metricsMiddleware(mux), "checkout", otelhttp.WithFilter(',
        'otelhttp.NewHandler(annotateHTTPSpan(metricsMiddleware(mux)), "checkout", otelhttp.WithSpanNameFormatter(spanNameFromRequest), otelhttp.WithFilter(',
    )
    src = src.replace(
        'otelhttp.NewHandler(metricsMiddleware(mux), "api-gateway", otelhttp.WithFilter(',
        'otelhttp.NewHandler(annotateHTTPSpan(metricsMiddleware(mux)), "api-gateway", otelhttp.WithSpanNameFormatter(spanNameFromRequest), otelhttp.WithFilter(',
    )
    src = src.replace(
        'otelhttp.NewHandler(metricsMiddleware(mux), "frontend", otelhttp.WithFilter(',
        'otelhttp.NewHandler(annotateHTTPSpan(metricsMiddleware(mux)), "frontend", otelhttp.WithSpanNameFormatter(spanNameFromRequest), otelhttp.WithFilter(',
    )
    return src


def patch(path: pathlib.Path) -> bool:
    src = path.read_text()
    original = src
    if "func initTracer()" not in src:
        return False
    src = ensure_imports(src)
    src = insert_helpers(src)
    if OLD_OPTS in src:
        src = src.replace(OLD_OPTS, NEW_OPTS)
    src = wrap_handler(src)
    if src != original:
        path.write_text(src)
        return True
    return False


def main() -> None:
    changed = []
    for main_go in sorted(ROOT.glob("*/main.go")):
        if patch(main_go):
            changed.append(str(main_go.parent.name))
    print("patched", len(changed), "services")
    for name in changed:
        print(" ", name)


if __name__ == "__main__":
    main()
