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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "profile-service"
const listenAddr = ":8090"

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

func healthHandler(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"service": serviceName, "status": "ok"})
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

var userServiceURL string
var mediaServiceURL string
var loyaltyServiceURL string
var cacheServiceURL string

func getProfileHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	userID := strings.TrimPrefix(r.URL.Path, "/profile/")
	if userID == "" || userID == "preferences" {
		preferencesHandler(w, r)
		return
	}
	userData, _ := httpGet(userServiceURL + "/users/" + url.QueryEscape(userID))
	if userData == nil { userData = map[string]interface{}{"user_id": userID, "name": "Unknown"} }
	avatarData, _ := httpGet(mediaServiceURL + "/media/avatar?user_id=" + url.QueryEscape(userID))
	if avatarData == nil { avatarData = map[string]interface{}{"url": "/default-avatar.png"} }
	// Fire-and-forget: get loyalty tier for profile
	go func() {
		_, err := httpGet(loyaltyServiceURL + "/loyalty/tier?user_id=" + url.QueryEscape(userID))
		if err != nil {
			log.Printf("WARNING: loyalty-service call failed: %v", err)
		}
	}()
	// Fire-and-forget: cache profile data
	go func() {
		_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("profile:"+userID) + "&value=" + url.QueryEscape(userID) + "&ttl=300")
		if err != nil {
			log.Printf("WARNING: cache-service call failed: %v", err)
		}
	}()
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"user_id": userID, "user": userData, "avatar": avatarData,
	})
}

func preferencesHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	userID := r.URL.Query().Get("user_id")
	if userID == "" { userID = "user-1" }
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"user_id": userID, "category": "electronics", "price_range": "mid",
	})
}

func profileRouter(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/profile/preferences" {
		preferencesHandler(w, r)
		return
	}
	if strings.HasPrefix(path, "/profile/") {
		getProfileHandler(w, r)
		return
	}
	http.NotFound(w, r)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Starting %s on %s", serviceName, listenAddr)

	userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")
	mediaServiceURL = getEnv("MEDIA_SERVICE_URL", "http://media-service:8114")
	loyaltyServiceURL = getEnv("LOYALTY_SERVICE_URL", "http://loyalty-service:8104")
	cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware(healthHandler))
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.HandleFunc("/profile/", metricsMiddleware(profileRouter))

	log.Printf("%s listening on %s", serviceName, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
