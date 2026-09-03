package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"context"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "catalog-service"
const listenAddr = ":8102"

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests"},
		[]string{"method", "endpoint", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: 200}
		next(rec, r)
		duration := time.Since(start).Seconds()
		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, fmt.Sprintf("%d", rec.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
	}
}

func jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func httpGet(ctx context.Context, url string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := otelHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

func httpPost(ctx context.Context, url string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := otelHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}


func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Fault injection (thread-safe)
type injectionConfig struct {
	mu        sync.RWMutex
	LatencyMs int
	ErrorRate float64
}

var faultCfg = &injectionConfig{}

func (c *injectionConfig) get() (int, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LatencyMs, c.ErrorRate
}

func (c *injectionConfig) set(latMs *int, errRate *float64) (int, float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if latMs != nil {
		v := *latMs; if v < 0 { v = 0 }; c.LatencyMs = v
	}
	if errRate != nil {
		v := *errRate; if v < 0 { v = 0 }; if v > 1 { v = 1 }; c.ErrorRate = v
	}
	return c.LatencyMs, c.ErrorRate
}

func adminConfigHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jsonResponse(w, http.StatusOK, map[string]interface{}{"latency_ms": faultCfg.LatencyMs, "error_rate": faultCfg.ErrorRate})
	case http.MethodPost:
		var latPtr *int
		var errPtr *float64
		if v := r.URL.Query().Get("latency_ms"); v != "" {
			if n, err := strconv.Atoi(v); err == nil { latPtr = &n }
		}
		if v := r.URL.Query().Get("error_rate"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil { errPtr = &f }
		}
		latMs, errRate := faultCfg.set(latPtr, errPtr)
		log.Printf("WARN: Injection config updated: latency_ms=%d error_rate=%.4f", latMs, errRate)
		jsonResponse(w, http.StatusOK, map[string]interface{}{"latency_ms": latMs, "error_rate": errRate})
	default:
		jsonResponse(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func applyFaultInjection(w http.ResponseWriter) bool {
	latMs, errRate := faultCfg.get()
	if latMs > 0 {
		time.Sleep(time.Duration(latMs) * time.Millisecond)
	}
	if errRate > 0 && rand.Float64() < errRate {
		jsonResponse(w, http.StatusInternalServerError, map[string]string{"error": "injected fault"})
		return true
	}
	return false
}


var otelHTTPClient = &http.Client{
	Timeout:   5 * time.Second,
	Transport: otelhttp.NewTransport(http.DefaultTransport),
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
		name = serviceName
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

func healthHandler(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"service": serviceName, "status": "ok"})
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

var reviewServiceURL string
var mediaServiceURL string
var cacheServiceURL string
var pricingServiceURL string
var inventoryServiceURL string

type Product struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	Category string  `json:"category"`
}

var products []Product

func initProducts() {
	categories := []string{"electronics", "clothing", "food", "books"}
	products = make([]Product, 100)
	for i := 0; i < 100; i++ {
		products[i] = Product{
			ID:       fmt.Sprintf("prod-%d", i+1),
			Name:     fmt.Sprintf("Product %d", i+1),
			Price:    float64(int(rand.Float64()*19500+500)) / 100.0,
			Category: categories[rand.Intn(len(categories))],
		}
	}
}

func listProductsHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	page := 1
	limit := 10
	fmt.Sscanf(r.URL.Query().Get("page"), "%d", &page)
	fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
	category := r.URL.Query().Get("category")
	// Check cache
	cached, err := httpGet(r.Context(), cacheServiceURL + "/cache/get?key=" + url.QueryEscape(fmt.Sprintf("catalog:%s:%d", category, page)))
	if err == nil {
		if hit, ok := cached["hit"].(bool); ok && hit {
			jsonResponse(w, http.StatusOK, cached["data"])
			return
		}
	}
	filtered := make([]Product, 0)
	for _, p := range products {
		if category == "" || p.Category == category {
			filtered = append(filtered, p)
		}
	}
	start := (page - 1) * limit
	end := start + limit
	if start > len(filtered) { start = len(filtered) }
	if end > len(filtered) { end = len(filtered) }
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"page": page, "products": filtered[start:end],
	})
}

func getProductHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	productID := strings.TrimPrefix(r.URL.Path, "/catalog/product/")
	var found *Product
	for i := range products {
		if products[i].ID == productID {
			found = &products[i]
			break
		}
	}
	if found == nil {
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}
	reviewData, _ := httpGet(r.Context(), reviewServiceURL + "/reviews/" + url.QueryEscape(productID))
	if reviewData == nil { reviewData = map[string]interface{}{"reviews": []interface{}{}} }
	mediaData, _ := httpGet(r.Context(), mediaServiceURL + "/media/product/" + url.QueryEscape(productID))
	if mediaData == nil { mediaData = map[string]interface{}{"images": []interface{}{}} }
	// Fire-and-forget: get real-time pricing
	go func() {
		_, err := httpGet(context.Background(), pricingServiceURL + "/pricing/calculate?items=" + url.QueryEscape(productID) + "&user_id=user-1")
		if err != nil {
			log.Printf("WARNING: pricing-service call failed: %v", err)
		}
	}()
	// Fire-and-forget: check inventory stock status
	go func() {
		_, err := httpGet(context.Background(), inventoryServiceURL + "/inventory/status?order_id=" + url.QueryEscape(productID))
		if err != nil {
			log.Printf("WARNING: inventory-service call failed: %v", err)
		}
	}()
	result := map[string]interface{}{
		"id": found.ID, "name": found.Name, "price": found.Price, "category": found.Category,
		"reviews": reviewData["reviews"], "media": mediaData,
	}
	jsonResponse(w, http.StatusOK, result)
}

func catalogRouter(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/catalog/products" {
		listProductsHandler(w, r)
		return
	}
	if strings.HasPrefix(path, "/catalog/product/") {
		getProductHandler(w, r)
		return
	}
	http.NotFound(w, r)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Starting %s on %s", serviceName, listenAddr)

	initProducts()
	reviewServiceURL = getEnv("REVIEW_SERVICE_URL", "http://review-service:8103")
	mediaServiceURL = getEnv("MEDIA_SERVICE_URL", "http://media-service:8114")
	cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")
	pricingServiceURL = getEnv("PRICING_SERVICE_URL", "http://pricing-service:8097")
	inventoryServiceURL = getEnv("INVENTORY_SERVICE_URL", "http://inventory-service:8086")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware(healthHandler))
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.HandleFunc("/catalog/", metricsMiddleware(catalogRouter))
	mux.HandleFunc("/catalog/products", metricsMiddleware(listProductsHandler))

	shutdownTracer := initTracer()
	defer shutdownTracer()
	handler := otelhttp.NewHandler(mux, serviceName, otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/metrics" && r.URL.Path != "/health"
	}))
	log.Printf("%s listening on %s", serviceName, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, handler))
}
