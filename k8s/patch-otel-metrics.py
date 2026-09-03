#!/usr/bin/env python3
"""Add OTLP metric export (http.server.request.duration) to every Go service."""
from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "environment" / "services"

OLD = """	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(name),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}"""

NEW = """	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(name),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	var mp *sdkmetric.MeterProvider
	mexp, merr := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithInsecure(),
		otlpmetrichttp.WithEndpoint(otlpHTTPHostPort(endpoint)),
	)
	if merr != nil {
		log.Printf("otel metric exporter init failed: %v", merr)
	} else {
		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp)),
			sdkmetric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
	}
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
		if mp != nil {
			_ = mp.Shutdown(shutdownCtx)
		}
	}"""

IMPORT_METRIC_EXP = '\t"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"\n'
IMPORT_SDKMETRIC = '\tsdkmetric "go.opentelemetry.io/otel/sdk/metric"\n'


def ensure_imports(src: str) -> str:
    if "otlpmetrichttp" not in src:
        src = src.replace(
            '\t"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"\n',
            '\t"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"\n' + IMPORT_METRIC_EXP,
        )
    if "sdkmetric" not in src:
        src = src.replace(
            '\tsdktrace "go.opentelemetry.io/otel/sdk/trace"\n',
            '\tsdkmetric "go.opentelemetry.io/otel/sdk/metric"\n\tsdktrace "go.opentelemetry.io/otel/sdk/trace"\n',
        )
    return src


def main() -> None:
    changed = 0
    for path in sorted(ROOT.glob("*/main.go")):
        src = path.read_text()
        if "otlpmetrichttp.New" in src:
            print("skip", path.parent.name)
            continue
        if OLD not in src:
            print("NO MATCH", path.parent.name)
            continue
        src = ensure_imports(src)
        src = src.replace(OLD, NEW, 1)
        path.write_text(src)
        print("patched", path.parent.name)
        changed += 1
    print("changed", changed)


if __name__ == "__main__":
    main()
