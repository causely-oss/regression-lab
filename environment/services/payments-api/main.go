// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ── Prometheus metrics ──────────────────────────────────────────────────────

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"method", "endpoint"},
	)

	dbQueryDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Payments DB query latency",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
	)

	dbPoolConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "db_pool_connections",
			Help: "Active DB pool connections",
		},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(dbQueryDuration)
	prometheus.MustRegister(dbPoolConnections)
}

// ── Global pool ─────────────────────────────────────────────────────────────

var pool *pgxpool.Pool

// ── Helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}

// ── Metrics middleware ──────────────────────────────────────────────────────

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start).Seconds()

		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rec.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(elapsed)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

// ── Handlers ────────────────────────────────────────────────────────────────

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "payments-api",
		"status":  "ok",
	})
}

func processPaymentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	checkoutID := r.URL.Query().Get("checkout_id")
	amountStr := r.URL.Query().Get("amount")

	if checkoutID == "" {
		writeError(w, http.StatusBadRequest, "Invalid checkout_id or amount")
		return
	}

	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil || amount <= 0 {
		writeError(w, http.StatusBadRequest, "Invalid checkout_id or amount")
		return
	}

	// Execute DB query with 15s timeout
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	dbStart := time.Now()

	var paymentID int
	var rowCheckoutID string
	var status string
	var dbLatencyMs float64

	err = pool.QueryRow(ctx,
		"SELECT * FROM process_payment($1, $2)",
		checkoutID, amount,
	).Scan(&paymentID, &rowCheckoutID, &status, &dbLatencyMs)

	dbElapsed := time.Since(dbStart).Seconds()
	dbQueryDuration.Observe(dbElapsed)

	// Update pool stats
	stat := pool.Stat()
	dbPoolConnections.Set(float64(stat.TotalConns()))

	if err != nil {
		errMsg := err.Error()

		// Pool exhausted: pgxpool returns an error when it cannot acquire a connection
		if strings.Contains(errMsg, "pool") && (strings.Contains(errMsg, "closed") || strings.Contains(errMsg, "exhaust")) ||
			strings.Contains(errMsg, "too many") ||
			strings.Contains(errMsg, "max connections") {
			log.Printf("ERROR DB connection pool exhausted: %v", err)
			writeError(w, http.StatusServiceUnavailable, "Database unavailable (pool exhausted)")
			return
		}

		// Timeout
		if ctx.Err() == context.DeadlineExceeded || strings.Contains(errMsg, "timeout") {
			log.Printf("ERROR DB query timed out for checkout_id=%s", checkoutID)
			writeError(w, http.StatusGatewayTimeout, "Database query timed out")
			return
		}

		log.Printf("ERROR DB error for checkout_id=%s: %v", checkoutID, err)
		writeError(w, http.StatusInternalServerError, "Payment processing failed")
		return
	}

	if dbElapsed > 1.0 {
		log.Printf("WARNING SLOW DB QUERY: %.3fs for checkout_id=%s", dbElapsed, checkoutID)
	}

	latencyMs := math.Round(dbElapsed*1000*10) / 10 // round to 1 decimal

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"payment_id":    paymentID,
		"checkout_id":   rowCheckoutID,
		"status":        status,
		"db_latency_ms": latencyMs,
	})
}


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

func initTracer() func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func() {}
	}
	ctx := context.Background()
	opts := []otlptracehttp.Option{
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(otlpHTTPHostPort(endpoint)),
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		log.Printf("otel exporter init failed: %v", err)
		return func() {}
	}
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = "payments-api"
	}
	res := resource.NewWithAttributes(
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
	}
}

// ── Main ────────────────────────────────────────────────────────────────────

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgresql://postgres:password@payments-db:5432/payments"
	}

	// pgxpool accepts postgres:// scheme; normalise if needed
	databaseURL = strings.Replace(databaseURL, "postgresql://", "postgres://", 1)

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Fatalf("Unable to parse DATABASE_URL: %v", err)
	}
	cfg.MinConns = 5
	cfg.MaxConns = 30

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	log.Println("DB pool created")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/payments/process", processPaymentHandler)
	mux.Handle("/metrics", promhttp.Handler())

	shutdownTracer := initTracer()
	defer shutdownTracer()

	handler := otelhttp.NewHandler(metricsMiddleware(mux), "payments-api", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/metrics" && r.URL.Path != "/health"
	}))

	srv := &http.Server{
		Addr:    ":8083",
		Handler: handler,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
		pool.Close()
		log.Println("DB pool closed")
	}()

	log.Println(fmt.Sprintf("payments-api listening on :8083"))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
