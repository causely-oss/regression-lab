// checkout -- orchestrates the checkout flow.
//
// 1. Reads cart from Redis (cart cache)
// 2. Calls pricing-service for price calculation
// 3. Calls fraud-detection for risk check
// 4. Calls payments-api to process payment
// 5. Calls billing-service for invoice
// 6. Publishes order to Kafka (orders topic)
//
// When payments-api is slow or erroring, checkout error rate rises.
// A configurable timeout on payments-api calls triggers a 503 back to the caller.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	rng "math/rand"
	"net/http"
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
	paymentsAPIURL   = envOrDefault("PAYMENTS_API_URL", "http://payments-api:8083")
	pricingServiceURL = envOrDefault("PRICING_SERVICE_URL", "http://pricing-service:8097")
	billingServiceURL = envOrDefault("BILLING_SERVICE_URL", "http://billing-service:8091")
	fraudDetectionURL = envOrDefault("FRAUD_DETECTION_URL", "http://fraud-detection:8111")
	redisURL          = envOrDefault("REDIS_URL", "redis://redis:6379/0")
	kafkaBrokers      = envOrDefault("KAFKA_BROKERS", "kafka:9092")
	paymentsTimeout   time.Duration
)

func init() {
	sec, err := strconv.ParseFloat(envOrDefault("PAYMENTS_TIMEOUT_SECONDS", "3.0"), 64)
	if err != nil {
		sec = 3.0
	}
	paymentsTimeout = time.Duration(sec * float64(time.Second))
}

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

var (
	httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests",
	}, []string{"method", "endpoint", "status"})

	httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"method", "endpoint"})

	checkoutOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "checkout_outcomes_total",
		Help: "Checkout success/failure counts",
	}, []string{"outcome"})
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration, checkoutOutcomes)
}

// ---------------------------------------------------------------------------
// Fault injection (thread-safe)
// ---------------------------------------------------------------------------

type injectionConfig struct {
	mu        sync.RWMutex
	LatencyMs int     `json:"latency_ms"`
	ErrorRate float64 `json:"error_rate"`
}

var inject = &injectionConfig{}

func (c *injectionConfig) get() (int, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LatencyMs, c.ErrorRate
}

func (c *injectionConfig) set(latMs *int, errRate *float64) (int, float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if latMs != nil {
		v := *latMs
		if v < 0 {
			v = 0
		}
		c.LatencyMs = v
	}
	if errRate != nil {
		v := *errRate
		if v < 0 {
			v = 0
		}
		if v > 1 {
			v = 1
		}
		c.ErrorRate = v
	}
	return c.LatencyMs, c.ErrorRate
}

func (c *injectionConfig) snapshot() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return map[string]interface{}{
		"latency_ms": c.LatencyMs,
		"error_rate": c.ErrorRate,
	}
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

var (
	rdb           *redis.Client
	kafkaProducer sarama.SyncProducer
)

func initRedis() {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("WARN: cannot parse REDIS_URL: %v", err)
		return
	}
	opts.DialTimeout = 2 * time.Second
	rdb = redis.NewClient(opts)
}

func initKafka() {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Timeout = 3 * time.Second
	cfg.Net.DialTimeout = 3 * time.Second
	brokers := strings.Split(kafkaBrokers, ",")
	var err error
	kafkaProducer, err = sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Printf("WARN: Kafka not available: %v -- orders will be skipped", err)
		kafkaProducer = nil
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func generateCheckoutID() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		// fallback
		return fmt.Sprintf("chk-%012x", rng.Int63())
	}
	return "chk-" + hex.EncodeToString(b)
}

func randomAmount() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(49001))
	amt := 10.0 + float64(n.Int64())/100.0
	return math.Round(amt*100) / 100
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// Metrics middleware
// ---------------------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start).Seconds()
		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rec.status)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(elapsed)
	})
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "checkout",
		"status":  "ok",
	})
}

func checkoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}

	checkoutID := generateCheckoutID()
	amount := randomAmount()

	// -- Fault injection --
	latMs, errRate := inject.get()
	if latMs > 0 {
		time.Sleep(time.Duration(latMs) * time.Millisecond)
	}
	if errRate > 0 && rng.Float64() < errRate {
		checkoutOutcomes.WithLabelValues("injected_error").Inc()
		log.Printf("ERROR: INJECTED ERROR in checkout for checkout_id=%s", checkoutID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "Internal server error"})
		return
	}

	// -- 1. Read cart from Redis --
	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cartKey := "cart:" + userID
		cart, err := rdb.Get(ctx, cartKey).Result()
		if err == redis.Nil || cart == "" {
			// cart miss -- populate
			cartData, _ := json.Marshal(map[string]interface{}{
				"items": []string{"item-1", "item-2"},
				"total": amount,
			})
			rdb.SetEx(ctx, cartKey, string(cartData), 300*time.Second)
			checkoutOutcomes.WithLabelValues("cart_miss").Inc()
		} else if err != nil {
			log.Printf("WARN: Redis unavailable: %v", err)
		} else {
			checkoutOutcomes.WithLabelValues("cart_hit").Inc()
		}
	}

	// -- 2. Call pricing-service --
	var pricingData map[string]interface{}
	func() {
		client := &http.Client{Timeout: 5 * time.Second}
		req, err := http.NewRequest(http.MethodGet, pricingServiceURL+"/pricing/calculate", nil)
		if err != nil {
			log.Printf("WARN: pricing request build error: %v", err)
			return
		}
		q := req.URL.Query()
		q.Set("items", "item-1,item-2")
		q.Set("user_id", userID)
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("WARN: Pricing service unavailable: %v", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			json.Unmarshal(body, &pricingData)
			if ft, ok := pricingData["final_total"]; ok {
				switch v := ft.(type) {
				case float64:
					amount = v
				case json.Number:
					if f, err := v.Float64(); err == nil {
						amount = f
					}
				}
			}
		}
	}()

	// -- 3. Fraud check --
	func() {
		client := &http.Client{Timeout: 3 * time.Second}
		req, err := http.NewRequest(http.MethodPost, fraudDetectionURL+"/fraud/check", nil)
		if err != nil {
			log.Printf("WARN: fraud request build error: %v", err)
			return
		}
		q := req.URL.Query()
		q.Set("user_id", userID)
		q.Set("amount", fmt.Sprintf("%.2f", amount))
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("WARN: Fraud detection unavailable: %v", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var fraudData map[string]interface{}
			body, _ := io.ReadAll(resp.Body)
			json.Unmarshal(body, &fraudData)
			if risk, ok := fraudData["risk"].(string); ok && risk == "high" {
				checkoutOutcomes.WithLabelValues("fraud_blocked").Inc()
				writeJSON(w, http.StatusForbidden, map[string]string{
					"detail": "Transaction blocked by fraud detection",
				})
				// signal caller to return
				panic("fraud_blocked")
			}
		}
	}()

	// -- 4. Call payments-api --
	var paymentData map[string]interface{}
	func() {
		client := &http.Client{Timeout: paymentsTimeout}
		req, err := http.NewRequest(http.MethodPost, paymentsAPIURL+"/payments/process", nil)
		if err != nil {
			checkoutOutcomes.WithLabelValues("payment_error").Inc()
			log.Printf("ERROR: payment request build error: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "Internal error"})
			panic("payment_build_error")
		}
		q := req.URL.Query()
		q.Set("checkout_id", checkoutID)
		q.Set("amount", fmt.Sprintf("%.2f", amount))
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			if os.IsTimeout(err) || strings.Contains(err.Error(), "deadline exceeded") ||
				strings.Contains(err.Error(), "Client.Timeout") {
				checkoutOutcomes.WithLabelValues("payment_timeout").Inc()
				log.Printf("ERROR: payments-api timed out for %s (timeout=%.1fs)",
					checkoutID, paymentsTimeout.Seconds())
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"detail": "Payment service timed out",
				})
				panic("payment_timeout")
			}
			checkoutOutcomes.WithLabelValues("payment_error").Inc()
			log.Printf("ERROR: Unexpected error calling payments-api: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "Internal error"})
			panic("payment_error")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			checkoutOutcomes.WithLabelValues("payment_error").Inc()
			log.Printf("ERROR: payments-api returned %d for %s", resp.StatusCode, checkoutID)
			writeJSON(w, http.StatusBadGateway, map[string]string{"detail": "Payment processing failed"})
			panic("payment_non200")
		}
		body, _ := io.ReadAll(resp.Body)
		json.Unmarshal(body, &paymentData)
	}()

	// -- 5. Call billing-service --
	var billingData map[string]interface{}
	func() {
		client := &http.Client{Timeout: 8 * time.Second}
		req, err := http.NewRequest(http.MethodPost, billingServiceURL+"/billing/charge", nil)
		if err != nil {
			log.Printf("WARN: billing request build error: %v", err)
			return
		}
		q := req.URL.Query()
		q.Set("checkout_id", checkoutID)
		q.Set("amount", fmt.Sprintf("%.2f", amount))
		q.Set("user_id", userID)
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("WARN: Billing service unavailable: %v", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			json.Unmarshal(body, &billingData)
		}
	}()

	// -- 6. Publish order to Kafka --
	if kafkaProducer != nil {
		orderMsg, _ := json.Marshal(map[string]interface{}{
			"checkout_id": checkoutID,
			"user_id":     userID,
			"amount":      amount,
			"payment_id":  paymentData["payment_id"],
		})
		msg := &sarama.ProducerMessage{
			Topic: "orders",
			Value: sarama.ByteEncoder(orderMsg),
		}
		if _, _, err := kafkaProducer.SendMessage(msg); err != nil {
			log.Printf("WARN: Failed to publish order to Kafka: %v", err)
		}
	}

	// -- Success --
	checkoutOutcomes.WithLabelValues("success").Inc()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"checkout_id": checkoutID,
		"status":      "success",
		"amount":      amount,
		"payment":     paymentData,
		"billing":     billingData,
		"pricing":     pricingData,
	})
}

// safeCheckoutHandler wraps checkoutHandler to recover from panic-based early returns
// used in the nested closures for fraud/payment error paths.
func safeCheckoutHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rv := recover(); rv != nil {
			// Response already written by the panicking closure.
			log.Printf("INFO: checkout early exit: %v", rv)
		}
	}()
	checkoutHandler(w, r)
}

func adminConfigHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, inject.snapshot())

	case http.MethodPost:
		var latPtr *int
		var errPtr *float64

		if v := r.URL.Query().Get("latency_ms"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				latPtr = &n
			}
		}
		if v := r.URL.Query().Get("error_rate"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				errPtr = &f
			}
		}
		latMs, errRate := inject.set(latPtr, errPtr)
		log.Printf("WARN: Injection config updated: latency_ms=%d error_rate=%.4f", latMs, errRate)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"latency_ms": latMs,
			"error_rate": errRate,
		})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("INFO: checkout service starting on :8082")

	initRedis()
	initKafka()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/checkout", safeCheckoutHandler)
	mux.HandleFunc("/admin/config", adminConfigHandler)
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         ":8082",
		Handler:      metricsMiddleware(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
