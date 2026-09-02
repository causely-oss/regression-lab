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
	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const serviceName = "recommendation-service"
const listenAddr = ":8095"

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

var recsServed = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "recommendations_served_total", Help: "Recommendations served"},
	[]string{"source"},
)

func init() {
	prometheus.MustRegister(recsServed)
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

var analyticsServiceURL string
var deliveryServiceURL string
var userServiceURL string
var cacheServiceURL string
var ctx = context.Background()

func consumeKafka(brokers, topic, groupID string) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	for {
		consumer, err := sarama.NewConsumer(strings.Split(brokers, ","), config)
		if err != nil {
			log.Printf("Kafka consumer error: %v -- retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		partConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Kafka partition consumer error: %v -- retrying in 5s", err)
			consumer.Close()
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Kafka consumer connected to '%s' topic", topic)
		for msg := range partConsumer.Messages() {
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Value, &data); err != nil {
				continue
			}
			userID, _ := data["user_id"].(string)
			if userID == "" { userID = "unknown" }
			recsServed.WithLabelValues("kafka").Inc()
			items := make([]string, 5)
			for i := range items { items[i] = fmt.Sprintf("rec-%d", rand.Intn(100)+1) }
			if redisClient != nil {
				data, _ := json.Marshal(map[string]interface{}{"items": items})
				redisClient.Set(ctx, "rec:"+userID, string(data), 5*time.Minute).Err()
			}
			httpPost(deliveryServiceURL + "/deliver?user_id=" + url.QueryEscape(userID) + "&type=recommendation")
		}
		partConsumer.Close()
		consumer.Close()
	}
}

func getRecommendationsHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	userID := strings.TrimPrefix(r.URL.Path, "/recommendations/")
	if userID == "" { userID = "user-1" }
	// Check Redis cache
	if redisClient != nil {
		val, err := redisClient.Get(ctx, "rec:"+userID).Result()
		if err == nil {
			recsServed.WithLabelValues("cache").Inc()
			var cached map[string]interface{}
			json.Unmarshal([]byte(val), &cached)
			jsonResponse(w, http.StatusOK, cached)
			return
		}
	}
	// Call analytics for user signals
	signals, _ := httpGet(analyticsServiceURL + "/analytics/signals?user_id=" + url.QueryEscape(userID))
	if signals == nil { signals = map[string]interface{}{} }
	// Fire-and-forget: get user personalization signals
	go func() {
		_, err := httpGet(userServiceURL + "/users/" + url.QueryEscape(userID))
		if err != nil {
			log.Printf("WARNING: user-service call failed: %v", err)
		}
	}()
	// Fire-and-forget: cache recommendation results
	go func() {
		_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("recs:"+userID) + "&value=" + url.QueryEscape(userID) + "&ttl=300")
		if err != nil {
			log.Printf("WARNING: cache-service call failed: %v", err)
		}
	}()
	recs := make([]map[string]interface{}, 10)
	for i := range recs {
		recs[i] = map[string]interface{}{
			"id": fmt.Sprintf("rec-%d", rand.Intn(100)+1),
			"score": float64(int(rand.Float64()*500+500)) / 1000.0,
		}
	}
	recsServed.WithLabelValues("computed").Inc()
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"user_id": userID, "recommendations": recs, "signals": signals,
	})
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Starting %s on %s", serviceName, listenAddr)

	analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")
	deliveryServiceURL = getEnv("DELIVERY_SERVICE_URL", "http://delivery-service:8096")
	userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")
	cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")
	redisURL := getEnv("REDIS_URL", "redis://redis:6379/3")
	initRedis(redisURL)
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	go consumeKafka(kafkaBrokers, "regression-lab-recommendations", "recommendation-service-group")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware(healthHandler))
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.HandleFunc("/recommendations/", metricsMiddleware(getRecommendationsHandler))

	log.Printf("%s listening on %s", serviceName, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
