package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "ingest-service"
const listenAddr = ":8093"

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

var eventsIngested = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "events_ingested_total", Help: "Events ingested"},
)

func init() {
	prometheus.MustRegister(eventsIngested)
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

var kafkaProducer sarama.SyncProducer

func initKafkaProducer(brokers string) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 3 * time.Second
	var err error
	for i := 0; i < 5; i++ {
		kafkaProducer, err = sarama.NewSyncProducer(strings.Split(brokers, ","), config)
		if err == nil {
			log.Printf("Kafka producer connected to %s", brokers)
			return
		}
		log.Printf("Kafka producer connect attempt %d failed: %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	log.Printf("WARNING: Kafka producer unavailable: %v", err)
}

func kafkaSend(topic string, data map[string]interface{}) {
	if kafkaProducer == nil {
		return
	}
	b, _ := json.Marshal(data)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
	_, _, err := kafkaProducer.SendMessage(msg)
	if err != nil {
		log.Printf("Kafka send to %s failed: %v", topic, err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"service": serviceName, "status": "ok"})
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func produceSynthetic() {
	types := []string{"click", "view", "purchase", "scroll"}
	for {
		if kafkaProducer != nil {
			kafkaSend("ingest-data", map[string]interface{}{
				"type":    types[rand.Intn(len(types))],
				"user_id": fmt.Sprintf("user-%d", rand.Intn(1000)+1),
				"ts":      float64(time.Now().UnixMilli()) / 1000,
			})
			eventsIngested.Inc()
		}
		time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
	}
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if applyFaultInjection(w) { return }
	eventType := r.URL.Query().Get("event_type")
	if eventType == "" { eventType = "click" }
	userID := r.URL.Query().Get("user_id")
	if userID == "" { userID = "user-1" }
	kafkaSend("ingest-data", map[string]interface{}{
		"type": eventType, "user_id": userID,
		"ts": float64(time.Now().UnixMilli()) / 1000,
	})
	eventsIngested.Inc()
	jsonResponse(w, http.StatusOK, map[string]interface{}{"status": "ingested", "type": eventType})
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Starting %s on %s", serviceName, listenAddr)

	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	go initKafkaProducer(kafkaBrokers)
	go produceSynthetic()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware(healthHandler))
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.HandleFunc("/ingest", metricsMiddleware(ingestHandler))

	log.Printf("%s listening on %s", serviceName, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
