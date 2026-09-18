// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

var (
	apiGatewayURL    string
	searchServiceURL string
	profileServiceURL string
	ingestServiceURL  string

	requestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	requestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"method", "endpoint"},
	)
)

type product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

var products []product

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(requestLatency)

	rng := rand.New(rand.NewSource(42))
	products = make([]product, 50)
	for i := 0; i < 50; i++ {
		products[i] = product{
			ID:    i + 1,
			Name:  fmt.Sprintf("Product %d", i+1),
			Price: math.Round((rng.Float64()*195+5)*100) / 100,
		}
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// metricsMiddleware wraps an http.Handler and records request count and latency.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start).Seconds()
		requestCount.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rec.statusCode)).Inc()
		requestLatency.WithLabelValues(r.Method, r.URL.Path).Observe(elapsed)
	})
}

// statusRecorder captures the HTTP status code written by handlers.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErrorJSON(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"detail": detail})
}

// proxyRequest performs an HTTP request to an upstream service and writes
// the response back to the client. It handles timeouts and upstream errors
// according to the shared error-handling rules.

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
		name = "frontend"
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

func proxyRequest(w http.ResponseWriter, r *http.Request, method, url string, timeout time.Duration, errorLabel string) {
	client := &http.Client{Timeout: timeout, Transport: otelhttp.NewTransport(http.DefaultTransport)}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		log.Printf("%s error building request: %v", errorLabel, err)
		writeErrorJSON(w, http.StatusBadGateway, errorLabel+" unavailable")
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		if os.IsTimeout(err) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "Client.Timeout") {
			writeErrorJSON(w, http.StatusGatewayTimeout, "Gateway timed out")
			return
		}
		log.Printf("%s error: %v", errorLabel, err)
		writeErrorJSON(w, http.StatusBadGateway, errorLabel+" unavailable")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		body, _ := io.ReadAll(resp.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
		return
	}

	// Copy upstream response headers and body.
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "frontend",
		"status":  "ok",
	})
}

func browseHandler(w http.ResponseWriter, r *http.Request) {
	pageStr := r.URL.Query().Get("page")
	page := 1
	if pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	start := (page - 1) * 10
	end := start + 10
	if start > len(products) {
		start = len(products)
	}
	if end > len(products) {
		end = len(products)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"page":     page,
		"products": products[start:end],
	})
}

func checkoutHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	url := fmt.Sprintf("%s/checkout?user_id=%s", apiGatewayURL, userID)
	proxyRequest(w, r, http.MethodPost, url, 12*time.Second, "Frontend")
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		q = "product"
	}
	pageStr := r.URL.Query().Get("page")
	if pageStr == "" {
		pageStr = "1"
	}
	url := fmt.Sprintf("%s/search?q=%s&page=%s", searchServiceURL, q, pageStr)
	proxyRequest(w, r, http.MethodGet, url, 10*time.Second, "Search")
}

func authLoginHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	password := r.URL.Query().Get("password")
	if password == "" {
		password = "pass"
	}
	url := fmt.Sprintf("%s/auth/login?user_id=%s&password=%s", apiGatewayURL, userID, password)
	proxyRequest(w, r, http.MethodPost, url, 10*time.Second, "Auth")
}

func ordersHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	url := fmt.Sprintf("%s/orders?user_id=%s", apiGatewayURL, userID)
	proxyRequest(w, r, http.MethodGet, url, 10*time.Second, "Orders")
}

func catalogProductHandler(w http.ResponseWriter, r *http.Request) {
	// Extract product_id from path: /catalog/product/{product_id}
	path := strings.TrimPrefix(r.URL.Path, "/catalog/product/")
	productID := strings.Split(path, "/")[0]
	if productID == "" {
		writeErrorJSON(w, http.StatusBadRequest, "product_id required")
		return
	}
	url := fmt.Sprintf("%s/catalog/product/%s", apiGatewayURL, productID)
	proxyRequest(w, r, http.MethodGet, url, 15*time.Second, "Catalog")
}

func cartGetHandler(w http.ResponseWriter, r *http.Request) {
	// Extract user_id from path: /cart/{user_id}
	userID := strings.TrimPrefix(r.URL.Path, "/cart/")
	if userID == "" || strings.Contains(userID, "/") {
		writeErrorJSON(w, http.StatusBadRequest, "user_id required")
		return
	}
	url := fmt.Sprintf("%s/cart/%s", apiGatewayURL, userID)
	proxyRequest(w, r, http.MethodGet, url, 5*time.Second, "Cart")
}

func cartAddHandler(w http.ResponseWriter, r *http.Request) {
	// Path: /cart/{user_id}/add?product_id=X
	trimmed := strings.TrimPrefix(r.URL.Path, "/cart/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 || parts[0] == "" {
		writeErrorJSON(w, http.StatusBadRequest, "user_id required")
		return
	}
	userID := parts[0]
	productID := r.URL.Query().Get("product_id")
	if productID == "" {
		productID = "prod-1"
	}
	url := fmt.Sprintf("%s/cart/%s/add?product_id=%s", apiGatewayURL, userID, productID)
	proxyRequest(w, r, http.MethodPost, url, 5*time.Second, "Cart")
}

func profileHandler(w http.ResponseWriter, r *http.Request) {
	// Extract user_id from path: /profile/{user_id}
	userID := strings.TrimPrefix(r.URL.Path, "/profile/")
	if userID == "" || strings.Contains(userID, "/") {
		writeErrorJSON(w, http.StatusBadRequest, "user_id required")
		return
	}
	url := fmt.Sprintf("%s/profile/%s", profileServiceURL, userID)
	proxyRequest(w, r, http.MethodGet, url, 5*time.Second, "Profile")
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	eventType := r.URL.Query().Get("event_type")
	if eventType == "" {
		eventType = "page_view"
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	url := fmt.Sprintf("%s/ingest?event_type=%s&user_id=%s", ingestServiceURL, eventType, userID)
	proxyRequest(w, r, http.MethodPost, url, 5*time.Second, "Ingest")
}

func main() {
	apiGatewayURL = envOrDefault("API_GATEWAY_URL", "http://api-gateway:8081")
	searchServiceURL = envOrDefault("SEARCH_SERVICE_URL", "http://search-service:8088")
	profileServiceURL = envOrDefault("PROFILE_SERVICE_URL", "http://profile-service:8090")
	ingestServiceURL = envOrDefault("INGEST_SERVICE_URL", "http://ingest-service:8093")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/browse", browseHandler)
	mux.HandleFunc("/checkout", checkoutHandler)
	mux.HandleFunc("/search", searchHandler)
	mux.HandleFunc("/auth/login", authLoginHandler)
	mux.HandleFunc("/orders", ordersHandler)
	mux.HandleFunc("/catalog/product/", catalogProductHandler)
	mux.HandleFunc("/cart/", func(w http.ResponseWriter, r *http.Request) {
		trimmed := strings.TrimPrefix(r.URL.Path, "/cart/")
		if strings.HasSuffix(trimmed, "/add") {
			cartAddHandler(w, r)
		} else {
			cartGetHandler(w, r)
		}
	})
	mux.HandleFunc("/profile/", profileHandler)
	mux.HandleFunc("/ingest", ingestHandler)
	mux.Handle("/metrics", promhttp.Handler())

	shutdownTracer := initTracer()
	defer shutdownTracer()
	handler := otelhttp.NewHandler(metricsMiddleware(mux), "frontend", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/metrics" && r.URL.Path != "/health"
	}))

	log.Println("frontend listening on :8080")
	if err := http.ListenAndServe(":8080", handler); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
