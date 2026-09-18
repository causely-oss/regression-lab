// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	checkoutURL    = envOrDefault("CHECKOUT_URL", "http://checkout:8082")
	authServiceURL = envOrDefault("AUTH_SERVICE_URL", "http://auth-service:8084")
	cartServiceURL = envOrDefault("CART_SERVICE_URL", "http://cart-service:8101")
	catalogURL     = envOrDefault("CATALOG_SERVICE_URL", "http://catalog-service:8102")
	ordersURL      = envOrDefault("ORDERS_SERVICE_URL", "http://orders-service:8085")
	profileURL     = envOrDefault("PROFILE_SERVICE_URL", "http://profile-service:8090")
)

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

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
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"method", "endpoint"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

// ---------------------------------------------------------------------------
// Metrics middleware
// ---------------------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start).Seconds()

		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rec.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(elapsed)
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"detail": msg})
}

// proxyRequest performs an HTTP request to an upstream service and writes the
// response back to the client.  Error handling follows the gateway convention:
//   - upstream status >= 500  -> forward that status code
//   - context deadline exceeded (timeout) -> 504
//   - any other error -> 502

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
		name = "api-gateway"
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

func proxyRequest(w http.ResponseWriter, r *http.Request, method, url string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		log.Printf("Gateway error building request: %v", err)
		writeError(w, http.StatusBadGateway, "Bad gateway")
		return
	}

	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			writeError(w, http.StatusGatewayTimeout, "Upstream service timed out")
			return
		}
		log.Printf("Gateway error: %v", err)
		writeError(w, http.StatusBadGateway, "Bad gateway")
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

	// Forward successful response
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "api-gateway",
		"status":  "ok",
	})
}

// POST /checkout?user_id=user-1
func checkoutHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	url := fmt.Sprintf("%s/checkout?user_id=%s", checkoutURL, userID)
	proxyRequest(w, r, http.MethodPost, url, 10*time.Second)
}

// POST /auth/login?user_id=user-1&password=pass
func authLoginHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	password := r.URL.Query().Get("password")
	if password == "" {
		password = "pass"
	}
	url := fmt.Sprintf("%s/auth/login?user_id=%s&password=%s", authServiceURL, userID, password)
	proxyRequest(w, r, http.MethodPost, url, 10*time.Second)
}

// POST /auth/validate?token=tok-123
func authValidateHandler(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		token = "tok-123"
	}
	url := fmt.Sprintf("%s/auth/validate?token=%s", authServiceURL, token)
	proxyRequest(w, r, http.MethodPost, url, 5*time.Second)
}

// GET /cart/{user_id}
func cartGetHandler(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimPrefix(r.URL.Path, "/cart/")
	if userID == "" || strings.Contains(userID, "/") {
		writeError(w, http.StatusBadRequest, "missing user_id")
		return
	}
	url := fmt.Sprintf("%s/cart/%s", cartServiceURL, userID)
	proxyRequest(w, r, http.MethodGet, url, 5*time.Second)
}

// POST /cart/{user_id}/add?product_id=prod-1
func cartAddHandler(w http.ResponseWriter, r *http.Request) {
	// Path: /cart/{user_id}/add
	trimmed := strings.TrimPrefix(r.URL.Path, "/cart/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "missing user_id")
		return
	}
	userID := parts[0]
	productID := r.URL.Query().Get("product_id")
	if productID == "" {
		productID = "prod-1"
	}
	url := fmt.Sprintf("%s/cart/%s/add?product_id=%s", cartServiceURL, userID, productID)
	proxyRequest(w, r, http.MethodPost, url, 5*time.Second)
}

// GET /catalog/products?page=1&category=
func catalogProductsHandler(w http.ResponseWriter, r *http.Request) {
	page := r.URL.Query().Get("page")
	if page == "" {
		page = "1"
	}
	category := r.URL.Query().Get("category")
	url := fmt.Sprintf("%s/catalog/products?page=%s&category=%s", catalogURL, page, category)
	proxyRequest(w, r, http.MethodGet, url, 10*time.Second)
}

// GET /catalog/product/{product_id}
func catalogProductHandler(w http.ResponseWriter, r *http.Request) {
	productID := strings.TrimPrefix(r.URL.Path, "/catalog/product/")
	if productID == "" || strings.Contains(productID, "/") {
		writeError(w, http.StatusBadRequest, "missing product_id")
		return
	}
	url := fmt.Sprintf("%s/catalog/product/%s", catalogURL, productID)
	proxyRequest(w, r, http.MethodGet, url, 15*time.Second)
}

// GET /orders?user_id=user-1
func ordersListHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	url := fmt.Sprintf("%s/orders?user_id=%s", ordersURL, userID)
	proxyRequest(w, r, http.MethodGet, url, 10*time.Second)
}

// GET /orders/{order_id}
func ordersGetHandler(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimPrefix(r.URL.Path, "/orders/")
	if orderID == "" || strings.Contains(orderID, "/") {
		writeError(w, http.StatusBadRequest, "missing order_id")
		return
	}
	url := fmt.Sprintf("%s/orders/%s", ordersURL, orderID)
	proxyRequest(w, r, http.MethodGet, url, 10*time.Second)
}

// GET /profile/{user_id}
func profileHandler(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimPrefix(r.URL.Path, "/profile/")
	if userID == "" || strings.Contains(userID, "/") {
		writeError(w, http.StatusBadRequest, "missing user_id")
		return
	}
	url := fmt.Sprintf("%s/profile/%s", profileURL, userID)
	proxyRequest(w, r, http.MethodGet, url, 5*time.Second)
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

func router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)
	mux.Handle("/metrics", promhttp.Handler())

	// Checkout
	mux.HandleFunc("/checkout", checkoutHandler)

	// Auth
	mux.HandleFunc("/auth/login", authLoginHandler)
	mux.HandleFunc("/auth/validate", authValidateHandler)

	// Catalog – order matters: longer prefix first so /catalog/product/ is
	// not swallowed by /catalog/products.
	mux.HandleFunc("/catalog/product/", catalogProductHandler)
	mux.HandleFunc("/catalog/products", catalogProductsHandler)

	// Cart & Orders need sub-path routing.  We use a single prefix handler
	// and dispatch inside based on the remainder of the path.
	mux.HandleFunc("/cart/", func(w http.ResponseWriter, r *http.Request) {
		// /cart/{user_id}/add  vs  /cart/{user_id}
		trimmed := strings.TrimPrefix(r.URL.Path, "/cart/")
		if strings.HasSuffix(trimmed, "/add") {
			cartAddHandler(w, r)
		} else {
			cartGetHandler(w, r)
		}
	})

	mux.HandleFunc("/orders/", ordersGetHandler)
	mux.HandleFunc("/orders", ordersListHandler)

	// Profile
	mux.HandleFunc("/profile/", profileHandler)

	return otelhttp.NewHandler(metricsMiddleware(mux), "api-gateway", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/metrics" && r.URL.Path != "/health"
	}))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	shutdownTracer := initTracer()
	defer shutdownTracer()
	addr := ":8081"
	log.Printf("api-gateway listening on %s", addr)
	log.Printf("  CHECKOUT_URL      = %s", checkoutURL)
	log.Printf("  AUTH_SERVICE_URL   = %s", authServiceURL)
	log.Printf("  CART_SERVICE_URL   = %s", cartServiceURL)
	log.Printf("  CATALOG_SERVICE_URL= %s", catalogURL)
	log.Printf("  ORDERS_SERVICE_URL = %s", ordersURL)
	log.Printf("  PROFILE_SERVICE_URL= %s", profileURL)

	srv := &http.Server{
		Addr:         addr,
		Handler:      router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
