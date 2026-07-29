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

const serviceName = "ranking-service"
const listenAddr = ":8089"

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

func httpGet(url string) (map[string]interface{}, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

func httpPost(url string) (map[string]interface{}, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", nil)
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

var profileServiceURL string
var analyticsServiceURL string
var recommendationServiceURL string
var ctx = context.Background()

func rankHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	q := r.URL.Query().Get("q")
	if q == "" { q = "product" }
	page := r.URL.Query().Get("page")
	if page == "" { page = "1" }
	pageNum := 1
	fmt.Sscanf(page, "%d", &pageNum)
	// Get user profile for personalization
	prefs, _ := httpGet(profileServiceURL + "/profile/preferences?user_id=user-1")
	if prefs == nil { prefs = map[string]interface{}{"category": "general"} }
	// Simulate ranking
	time.Sleep(time.Duration(2+rand.Intn(13)) * time.Millisecond)
	results := make([]map[string]interface{}, 0, 10)
	for i := (pageNum-1)*10 + 1; i <= pageNum*10; i++ {
		results = append(results, map[string]interface{}{
			"id": i, "name": fmt.Sprintf("Product %d", i),
			"score": float64(int(rand.Float64()*500+500)) / 1000.0,
		})
	}
	if redisClient != nil {
		data, _ := json.Marshal(results)
		redisClient.Set(ctx, fmt.Sprintf("rank:%s:%s", q, page), string(data), 60*time.Second).Err()
	}
	// Fire-and-forget: track ranking in analytics
	go func() {
		_, err := httpPost(analyticsServiceURL + "/analytics/signals?user_id=user-1&event=ranking&query=" + url.QueryEscape(q))
		if err != nil {
			log.Printf("WARNING: analytics-service call failed: %v", err)
		}
	}()
	// Fire-and-forget: get personalized boost from recommendations
	go func() {
		_, err := httpGet(recommendationServiceURL + "/recommendations/user-1")
		if err != nil {
			log.Printf("WARNING: recommendation-service call failed: %v", err)
		}
	}()
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"query": q, "page": pageNum, "results": results, "personalized": prefs != nil,
	})
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Starting %s on %s", serviceName, listenAddr)

	profileServiceURL = getEnv("PROFILE_SERVICE_URL", "http://profile-service:8090")
	analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")
	recommendationServiceURL = getEnv("RECOMMENDATION_SERVICE_URL", "http://recommendation-service:8095")
	redisURL := getEnv("REDIS_URL", "redis://redis:6379/2")
	initRedis(redisURL)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware(healthHandler))
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.HandleFunc("/rank", metricsMiddleware(rankHandler))

	log.Printf("%s listening on %s", serviceName, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
