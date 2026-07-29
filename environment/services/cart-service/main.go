package main

import (
	"context"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const serviceName = "cart-service"
const listenAddr = ":8101"

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

var httpClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	},
}

func httpGet(url string) (map[string]interface{}, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

func httpPost(url string) (map[string]interface{}, error) {
	resp, err := httpClient.Post(url, "application/json", nil)
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

var redisClient *redis.Client

func initRedis(redisURL string) {
	// Parse redis://host:port/db
	addr := "redis:6379"
	db := 0
	if strings.HasPrefix(redisURL, "redis://") {
		url := strings.TrimPrefix(redisURL, "redis://")
		parts := strings.SplitN(url, "/", 2)
		addr = parts[0]
		if len(parts) > 1 {
			fmt.Sscanf(parts[1], "%d", &db)
		}
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr:        addr,
		DB:          db,
		DialTimeout: 2 * time.Second,
	})
	log.Printf("Redis client configured for %s db=%d", addr, db)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"service": serviceName, "status": "ok"})
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

var catalogServiceURL string
var pricingServiceURL string
var sessionServiceURL string
var ctx = context.Background()

// Bounds concurrent fire-and-forget calls so a slow downstream can't
// accumulate goroutines/connections without limit under sustained load.
var backgroundCallSem = make(chan struct{}, 50)

func fireAndForget(name, url string) {
	select {
	case backgroundCallSem <- struct{}{}:
	default:
		log.Printf("WARNING: %s call skipped, background call limit reached", name)
		return
	}
	go func() {
		defer func() { <-backgroundCallSem }()
		if _, err := httpGet(url); err != nil {
			log.Printf("WARNING: %s call failed: %v", name, err)
		}
	}()
}

func getCartHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	// Extract user_id from /cart/{user_id} (but not /cart/{user_id}/add)
	path := strings.TrimPrefix(r.URL.Path, "/cart/")
	parts := strings.SplitN(path, "/", 2)
	userID := parts[0]
	if redisClient != nil {
		val, err := redisClient.Get(ctx, "cart:"+userID).Result()
		if err == nil {
			var cart map[string]interface{}
			json.Unmarshal([]byte(val), &cart)
			jsonResponse(w, http.StatusOK, cart)
			return
		}
	}
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"user_id": userID, "items": []interface{}{}, "total": 0,
	})
}

func addToCartHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	// /cart/{user_id}/add
	path := strings.TrimPrefix(r.URL.Path, "/cart/")
	parts := strings.SplitN(path, "/", 2)
	userID := parts[0]
	productID := r.URL.Query().Get("product_id")
	if productID == "" { productID = "prod-1" }
	// Validate product
	product, _ := httpGet(catalogServiceURL + "/catalog/product/" + url.QueryEscape(productID))
	if product == nil { product = map[string]interface{}{"id": productID, "price": 29.99} }
	cart := map[string]interface{}{"user_id": userID, "items": []interface{}{}, "total": 0.0}
	if redisClient != nil {
		val, err := redisClient.Get(ctx, "cart:"+userID).Result()
		if err == nil {
			json.Unmarshal([]byte(val), &cart)
		}
	}
	items, _ := cart["items"].([]interface{})
	items = append(items, product)
	cart["items"] = items
	total := 0.0
	for _, item := range items {
		if m, ok := item.(map[string]interface{}); ok {
			if p, ok := m["price"].(float64); ok { total += p }
		}
	}
	cart["total"] = total
	if redisClient != nil {
		data, _ := json.Marshal(cart)
		redisClient.Set(ctx, "cart:"+userID, string(data), time.Hour).Err()
	}
	// Fire-and-forget: get real-time pricing
	fireAndForget("pricing-service", pricingServiceURL+"/pricing/calculate?items="+url.QueryEscape(productID)+"&user_id="+url.QueryEscape(userID))
	// Fire-and-forget: validate session
	fireAndForget("session-service", sessionServiceURL+"/sessions/validate?token="+url.QueryEscape("sess-"+userID))
	jsonResponse(w, http.StatusOK, cart)
}

func cartRouter(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasSuffix(path, "/add") && r.Method == http.MethodPost {
		addToCartHandler(w, r)
		return
	}
	if strings.HasPrefix(path, "/cart/") {
		getCartHandler(w, r)
		return
	}
	http.NotFound(w, r)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Starting %s on %s", serviceName, listenAddr)

	catalogServiceURL = getEnv("CATALOG_SERVICE_URL", "http://catalog-service:8102")
	pricingServiceURL = getEnv("PRICING_SERVICE_URL", "http://pricing-service:8097")
	sessionServiceURL = getEnv("SESSION_SERVICE_URL", "http://session-service:8106")
	redisURL := getEnv("REDIS_URL", "redis://redis:6379/5")
	initRedis(redisURL)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware(healthHandler))
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.HandleFunc("/cart/", metricsMiddleware(cartRouter))

	log.Printf("%s listening on %s", serviceName, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
