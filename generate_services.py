#!/usr/bin/env python3
"""
Generator script: creates all 36 service main.go files, go.mod files, Dockerfiles,
updates k8s manifests, docker-compose, prometheus config, and build script.

All 32 templated services (everything except KEEP_EXISTING) are generated as Go
services using net/http, prometheus/client_golang, go-redis, and sarama.

Service Architecture (36 services):
  Auth:      frontend -> api-gateway -> auth-service -> session-service, user-service
  Orders:    checkout -> Kafka(regression-lab-orders) -> orders-service -> inventory-service -> Kafka(regression-lab-shipping-events) -> shipping-service -> warehouse-service
  Search:    frontend -> search-service -> ranking-service -> profile-service -> user-service
  Deep:      frontend -> api-gateway -> catalog-service -> review-service -> recommendation-service -> analytics-service -> reporting-service
  Billing:   checkout -> billing-service -> payment-adapter -> external-payment-api; billing-service -> tax-service, fraud-detection
  Streaming: ingest-service -> Kafka(regression-lab-ingest-data) -> processing-service -> Kafka(regression-lab-recommendations) -> recommendation-service -> delivery-service
  Orders UI: frontend -> api-gateway -> orders-service -> inventory-service -> warehouse-service
  Checkout:  checkout -> pricing-service -> discount-service -> loyalty-service

Kafka topics are prefixed with regression-lab- so they do not collide with
existing cluster topics (orders, notifications, etc.).
"""

import os
import textwrap

BASE = os.path.dirname(os.path.abspath(__file__))
SERVICES_DIR = os.path.join(BASE, "environment", "services")

# Prefix keeps demo topics unique on shared Kafka clusters.
KAFKA_TOPIC_ORDERS = "regression-lab-orders"
KAFKA_TOPIC_INVENTORY_UPDATES = "regression-lab-inventory-updates"
KAFKA_TOPIC_SHIPPING_EVENTS = "regression-lab-shipping-events"
KAFKA_TOPIC_NOTIFICATIONS = "regression-lab-notifications"
KAFKA_TOPIC_AUDIT_EVENTS = "regression-lab-audit-events"
KAFKA_TOPIC_ANALYTICS_EVENTS = "regression-lab-analytics-events"
KAFKA_TOPIC_INGEST_DATA = "regression-lab-ingest-data"
KAFKA_TOPIC_RECOMMENDATIONS = "regression-lab-recommendations"

# -- Service definitions -------------------------------------------------------

SERVICES = {
    # name: (port, needs_kafka_consumer, needs_kafka_producer, needs_redis)
    "frontend":              (8080, False, False, False),
    "api-gateway":           (8081, False, False, False),
    "checkout":              (8082, False, True,  True),
    "payments-api":          (8083, False, False, False),
    "auth-service":          (8084, False, True,  False),
    "orders-service":        (8085, True,  True,  False),
    "inventory-service":     (8086, True,  True,  True),
    "shipping-service":      (8087, True,  False, False),
    "search-service":        (8088, False, True,  False),
    "ranking-service":       (8089, False, False, True),
    "profile-service":       (8090, False, False, False),
    "billing-service":       (8091, False, True,  False),
    "payment-adapter":       (8092, False, False, False),
    "ingest-service":        (8093, False, True,  False),
    "processing-service":    (8094, True,  True,  False),
    "recommendation-service":(8095, True,  False, True),
    "delivery-service":      (8096, False, False, False),
    "pricing-service":       (8097, False, False, True),
    "discount-service":      (8098, False, False, False),
    "user-service":          (8099, False, False, False),
    "notification-service":  (8100, True,  False, False),
    "cart-service":          (8101, False, False, True),
    "catalog-service":       (8102, False, False, False),
    "review-service":        (8103, False, True,  False),
    "loyalty-service":       (8104, False, False, True),
    "audit-service":         (8105, True,  False, False),
    "session-service":       (8106, False, False, True),
    "analytics-service":     (8107, True,  False, False),
    "email-service":         (8108, False, False, False),
    "cache-service":         (8109, False, False, True),
    "reporting-service":     (8110, False, False, False),
    "fraud-detection":       (8111, False, False, True),
    "tax-service":           (8112, False, False, False),
    "warehouse-service":     (8113, False, True,  False),
    "media-service":         (8114, False, False, False),
    "external-payment-api":  (8115, False, False, False),
}

# Services that should NOT be regenerated (they already exist with custom logic)
KEEP_EXISTING = {"frontend", "api-gateway", "checkout", "payments-api"}


# -- go.mod generator ----------------------------------------------------------

GO_MODULE_PREFIX = "github.com/scenario01"

def gen_go_mod(svc_name, needs_kafka, needs_redis):
    """Generate go.mod for a service."""
    lines = [
        f"module {GO_MODULE_PREFIX}/{svc_name}",
        "",
        "go 1.25.0",
        "",
        "require (",
        '\tgithub.com/prometheus/client_golang v1.19.0',
    ]
    if needs_redis:
        lines.append('\tgithub.com/redis/go-redis/v9 v9.5.1')
    if needs_kafka:
        lines.append('\tgithub.com/IBM/sarama v1.43.1')
    lines.append(")")
    lines.append("")
    return "\n".join(lines)


# -- Dockerfile generator (Go multi-stage) ------------------------------------

def gen_dockerfile(svc_name, port):
    return textwrap.dedent(f"""\
    FROM golang:1.25-alpine AS builder
    WORKDIR /app
    COPY go.mod ./
    COPY main.go .
    RUN go mod tidy && go mod download
    RUN CGO_ENABLED=0 GOOS=linux go build -o service .

    FROM alpine:3.19
    RUN apk --no-cache add ca-certificates
    WORKDIR /app
    COPY --from=builder /app/service .
    EXPOSE {port}
    CMD ["./service"]
    """)


# -- Common Go helper snippets ------------------------------------------------

def go_header(svc_name, port, imports):
    """Standard package + import block."""
    extras = [
        "context",
        "strings",
        "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        "go.opentelemetry.io/otel",
        "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp",
        "go.opentelemetry.io/otel/propagation",
        "go.opentelemetry.io/otel/sdk/resource",
    ]
    imports = list(dict.fromkeys(list(imports) + extras))
    import_block = "\n".join(f'\t"{imp}"' for imp in imports)
    import_block += "\n\tsdktrace \"go.opentelemetry.io/otel/sdk/trace\"\n\tsemconv \"go.opentelemetry.io/otel/semconv/v1.26.0\""
    return f'''package main

import (
{import_block}
)

const serviceName = "{svc_name}"
const listenAddr = ":{port}"
'''


def go_prom_and_middleware():
    """Prometheus metrics vars and middleware wrapper."""
    return '''
var (
\thttpRequestsTotal = prometheus.NewCounterVec(
\t\tprometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests"},
\t\t[]string{"method", "endpoint", "status"},
\t)
\thttpRequestDuration = prometheus.NewHistogramVec(
\t\tprometheus.HistogramOpts{
\t\t\tName:    "http_request_duration_seconds",
\t\t\tHelp:    "HTTP request latency",
\t\t\tBuckets: prometheus.DefBuckets,
\t\t},
\t\t[]string{"method", "endpoint"},
\t)
)

func init() {
\tprometheus.MustRegister(httpRequestsTotal)
\tprometheus.MustRegister(httpRequestDuration)
}

type statusRecorder struct {
\thttp.ResponseWriter
\tstatusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
\tr.statusCode = code
\tr.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
\treturn func(w http.ResponseWriter, r *http.Request) {
\t\tstart := time.Now()
\t\trec := &statusRecorder{ResponseWriter: w, statusCode: 200}
\t\tnext(rec, r)
\t\tduration := time.Since(start).Seconds()
\t\thttpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, fmt.Sprintf("%d", rec.statusCode)).Inc()
\t\thttpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
\t}
}
'''


def go_json_helpers():
    """Helper to write JSON responses and proxy HTTP calls."""
    return '''
func jsonResponse(w http.ResponseWriter, status int, data interface{}) {
\tw.Header().Set("Content-Type", "application/json")
\tw.WriteHeader(status)
\tjson.NewEncoder(w).Encode(data)
}

var otelHTTPClient = &http.Client{
\tTimeout:   5 * time.Second,
\tTransport: otelhttp.NewTransport(http.DefaultTransport),
}

func initTracer() func() {
\tendpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
\tif endpoint == "" {
\t\treturn func() {}
\t}
\tctx := context.Background()
\topts := []otlptracehttp.Option{otlptracehttp.WithInsecure()}
\tif strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
\t\topts = append(opts, otlptracehttp.WithEndpointURL(endpoint))
\t} else {
\t\topts = append(opts, otlptracehttp.WithEndpoint(endpoint))
\t}
\texp, err := otlptracehttp.New(ctx, opts...)
\tif err != nil {
\t\tlog.Printf("otel exporter init failed: %v", err)
\t\treturn func() {}
\t}
\tname := os.Getenv("OTEL_SERVICE_NAME")
\tif name == "" {
\t\tname = serviceName
\t}
\ttp := sdktrace.NewTracerProvider(
\t\tsdktrace.WithBatcher(exp),
\t\tsdktrace.WithResource(resource.NewWithAttributes(
\t\t\tsemconv.SchemaURL,
\t\t\tsemconv.ServiceName(name),
\t\t)),
\t)
\totel.SetTracerProvider(tp)
\totel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
\t\tpropagation.TraceContext{},
\t\tpropagation.Baggage{},
\t))
\treturn func() {
\t\tshutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
\t\tdefer cancel()
\t\t_ = tp.Shutdown(shutdownCtx)
\t}
}

func httpGet(ctx context.Context, url string) (map[string]interface{}, error) {
\treq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
\tif err != nil {
\t\treturn nil, err
\t}
\tresp, err := otelHTTPClient.Do(req)
\tif err != nil {
\t\treturn nil, err
\t}
\tdefer resp.Body.Close()
\tvar result map[string]interface{}
\tjson.NewDecoder(resp.Body).Decode(&result)
\treturn result, nil
}

func httpPost(ctx context.Context, url string) (map[string]interface{}, error) {
\treq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
\tif err != nil {
\t\treturn nil, err
\t}
\treq.Header.Set("Content-Type", "application/json")
\tresp, err := otelHTTPClient.Do(req)
\tif err != nil {
\t\treturn nil, err
\t}
\tdefer resp.Body.Close()
\tvar result map[string]interface{}
\tjson.NewDecoder(resp.Body).Decode(&result)
\treturn result, nil
}

func getEnv(key, fallback string) string {
\tif v := os.Getenv(key); v != "" {
\t\treturn v
\t}
\treturn fallback
}
'''


def go_health_and_metrics(svc_name):
    """Standard /health and /metrics handlers."""
    return f'''
func healthHandler(w http.ResponseWriter, r *http.Request) {{
\tjsonResponse(w, http.StatusOK, map[string]string{{"service": serviceName, "status": "ok"}})
}}

func metricsHandler(w http.ResponseWriter, r *http.Request) {{
\tpromhttp.Handler().ServeHTTP(w, r)
}}
'''


def go_fault_injection():
    """Fault injection config and /admin/config handler."""
    return '''
// Fault injection (thread-safe)
type injectionConfig struct {
\tmu        sync.RWMutex
\tLatencyMs int
\tErrorRate float64
}

var faultCfg = &injectionConfig{}

func (c *injectionConfig) get() (int, float64) {
\tc.mu.RLock()
\tdefer c.mu.RUnlock()
\treturn c.LatencyMs, c.ErrorRate
}

func (c *injectionConfig) set(latMs *int, errRate *float64) (int, float64) {
\tc.mu.Lock()
\tdefer c.mu.Unlock()
\tif latMs != nil {
\t\tv := *latMs; if v < 0 { v = 0 }; c.LatencyMs = v
\t}
\tif errRate != nil {
\t\tv := *errRate; if v < 0 { v = 0 }; if v > 1 { v = 1 }; c.ErrorRate = v
\t}
\treturn c.LatencyMs, c.ErrorRate
}

func adminConfigHandler(w http.ResponseWriter, r *http.Request) {
\tswitch r.Method {
\tcase http.MethodGet:
\t\tjsonResponse(w, http.StatusOK, map[string]interface{}{"latency_ms": faultCfg.LatencyMs, "error_rate": faultCfg.ErrorRate})
\tcase http.MethodPost:
\t\tvar latPtr *int
\t\tvar errPtr *float64
\t\tif v := r.URL.Query().Get("latency_ms"); v != "" {
\t\t\tif n, err := strconv.Atoi(v); err == nil { latPtr = &n }
\t\t}
\t\tif v := r.URL.Query().Get("error_rate"); v != "" {
\t\t\tif f, err := strconv.ParseFloat(v, 64); err == nil { errPtr = &f }
\t\t}
\t\tlatMs, errRate := faultCfg.set(latPtr, errPtr)
\t\tjsonResponse(w, http.StatusOK, map[string]interface{}{"latency_ms": latMs, "error_rate": errRate})
\tdefault:
\t\tjsonResponse(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
\t}
}

func applyFaultInjection(ctx context.Context, w http.ResponseWriter) bool {
\tlatMs, errRate := faultCfg.get()
\tif latMs > 0 {
\t\tselect {
\t\tcase <-time.After(time.Duration(latMs) * time.Millisecond):
\t\tcase <-ctx.Done():
\t\t\treturn true
\t\t}
\t}
\tif errRate > 0 && rand.Float64() < errRate {
\t\tjsonResponse(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
\t\treturn true
\t}
\treturn false
}
'''


def go_fault_injection_inventory():
    """Fault injection for inventory-service with pause_consumer support."""
    return '''
// Fault injection (thread-safe)
type injectionConfig struct {
\tmu        sync.RWMutex
\tLatencyMs int
\tErrorRate float64
}

var faultCfg = &injectionConfig{}
var consumerPaused int32

func (c *injectionConfig) get() (int, float64) {
\tc.mu.RLock()
\tdefer c.mu.RUnlock()
\treturn c.LatencyMs, c.ErrorRate
}

func (c *injectionConfig) set(latMs *int, errRate *float64) (int, float64) {
\tc.mu.Lock()
\tdefer c.mu.Unlock()
\tif latMs != nil {
\t\tv := *latMs; if v < 0 { v = 0 }; c.LatencyMs = v
\t}
\tif errRate != nil {
\t\tv := *errRate; if v < 0 { v = 0 }; if v > 1 { v = 1 }; c.ErrorRate = v
\t}
\treturn c.LatencyMs, c.ErrorRate
}

func adminConfigHandler(w http.ResponseWriter, r *http.Request) {
\tswitch r.Method {
\tcase http.MethodGet:
\t\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t\t"latency_ms": faultCfg.LatencyMs, "error_rate": faultCfg.ErrorRate,
\t\t\t"pause_consumer": atomic.LoadInt32(&consumerPaused) != 0,
\t\t})
\tcase http.MethodPost:
\t\tvar latPtr *int
\t\tvar errPtr *float64
\t\tif v := r.URL.Query().Get("latency_ms"); v != "" {
\t\t\tif n, err := strconv.Atoi(v); err == nil { latPtr = &n }
\t\t}
\t\tif v := r.URL.Query().Get("error_rate"); v != "" {
\t\t\tif f, err := strconv.ParseFloat(v, 64); err == nil { errPtr = &f }
\t\t}
\t\tif v := r.URL.Query().Get("pause_consumer"); v != "" {
\t\t\tif v == "true" {
\t\t\t\tatomic.StoreInt32(&consumerPaused, 1)
\t\t\t\tlog.Printf("WARN: Kafka consumer PAUSED")
\t\t\t} else if v == "false" {
\t\t\t\tatomic.StoreInt32(&consumerPaused, 0)
\t\t\t\tlog.Printf("WARN: Kafka consumer RESUMED")
\t\t\t}
\t\t}
\t\tlatMs, errRate := faultCfg.set(latPtr, errPtr)
\t\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t\t"latency_ms": latMs, "error_rate": errRate,
\t\t\t"pause_consumer": atomic.LoadInt32(&consumerPaused) != 0,
\t\t})
\tdefault:
\t\tjsonResponse(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
\t}
}

func applyFaultInjection(ctx context.Context, w http.ResponseWriter) bool {
\tlatMs, errRate := faultCfg.get()
\tif latMs > 0 {
\t\tselect {
\t\tcase <-time.After(time.Duration(latMs) * time.Millisecond):
\t\tcase <-ctx.Done():
\t\t\treturn true
\t\t}
\t}
\tif errRate > 0 && rand.Float64() < errRate {
\t\tjsonResponse(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
\t\treturn true
\t}
\treturn false
}
'''


def go_kafka_producer_setup():
    """Kafka producer initialization helper."""
    return '''
var kafkaProducer sarama.SyncProducer

func initKafkaProducer(brokers string) {
\tconfig := sarama.NewConfig()
\tconfig.Producer.RequiredAcks = sarama.WaitForLocal
\tconfig.Producer.Return.Successes = true
\tconfig.Producer.Timeout = 3 * time.Second
\tvar err error
\tfor i := 0; i < 5; i++ {
\t\tkafkaProducer, err = sarama.NewSyncProducer(strings.Split(brokers, ","), config)
\t\tif err == nil {
\t\t\tlog.Printf("Kafka producer connected to %s", brokers)
\t\t\treturn
\t\t}
\t\tlog.Printf("Kafka producer connect attempt %d failed: %v", i+1, err)
\t\ttime.Sleep(3 * time.Second)
\t}
\tlog.Printf("WARNING: Kafka producer unavailable: %v", err)
}

func kafkaSend(topic string, data map[string]interface{}) {
\tif kafkaProducer == nil {
\t\treturn
\t}
\tb, _ := json.Marshal(data)
\tmsg := &sarama.ProducerMessage{
\t\tTopic: topic,
\t\tValue: sarama.ByteEncoder(b),
\t}
\t_, _, err := kafkaProducer.SendMessage(msg)
\tif err != nil {
\t\tlog.Printf("Kafka send to %s failed: %v", topic, err)
\t}
}
'''


def go_kafka_consumer_func(topic, group_id, body_code):
    """Generate a Kafka consumer goroutine function."""
    return f'''
func consumeKafka(brokers, topic, groupID string) {{
\tconfig := sarama.NewConfig()
\tconfig.Consumer.Offsets.Initial = sarama.OffsetNewest
\tconfig.Consumer.Return.Errors = true
\tfor {{
\t\tconsumer, err := sarama.NewConsumer(strings.Split(brokers, ","), config)
\t\tif err != nil {{
\t\t\tlog.Printf("Kafka consumer error: %v -- retrying in 5s", err)
\t\t\ttime.Sleep(5 * time.Second)
\t\t\tcontinue
\t\t}}
\t\tpartConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
\t\tif err != nil {{
\t\t\tlog.Printf("Kafka partition consumer error: %v -- retrying in 5s", err)
\t\t\tconsumer.Close()
\t\t\ttime.Sleep(5 * time.Second)
\t\t\tcontinue
\t\t}}
\t\tlog.Printf("Kafka consumer connected to '%s' topic", topic)
\t\tfor msg := range partConsumer.Messages() {{
\t\t\tvar data map[string]interface{{}}
\t\t\tif err := json.Unmarshal(msg.Value, &data); err != nil {{
\t\t\t\tcontinue
\t\t\t}}
{body_code}
\t\t}}
\t\tpartConsumer.Close()
\t\tconsumer.Close()
\t}}
}}
'''


def go_redis_setup():
    """Redis client creation from REDIS_URL."""
    return '''
var redisClient *redis.Client

func initRedis(redisURL string) {
\t// Parse redis://host:port/db
\taddr := "redis:6379"
\tdb := 0
\tif strings.HasPrefix(redisURL, "redis://") {
\t\turl := strings.TrimPrefix(redisURL, "redis://")
\t\tparts := strings.SplitN(url, "/", 2)
\t\taddr = parts[0]
\t\tif len(parts) > 1 {
\t\t\tfmt.Sscanf(parts[1], "%d", &db)
\t\t}
\t}
\tredisClient = redis.NewClient(&redis.Options{
\t\tAddr:        addr,
\t\tDB:          db,
\t\tDialTimeout: 2 * time.Second,
\t})
\tlog.Printf("Redis client configured for %s db=%d", addr, db)
}
'''


def go_main_func(setup_lines, route_lines):
    """Standard main function."""
    setup_code = "\n".join(f"\t{line}" for line in setup_lines)
    route_code = "\n".join(f"\t{line}" for line in route_lines)
    return f'''
func main() {{
\tlog.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
\tlog.Printf("Starting %s on %s", serviceName, listenAddr)

{setup_code}

\tmux := http.NewServeMux()
\tmux.HandleFunc("/health", metricsMiddleware(healthHandler))
\tmux.HandleFunc("/metrics", metricsHandler)
{route_code}

\tshutdownTracer := initTracer()
\tdefer shutdownTracer()
\thandler := otelhttp.NewHandler(mux, serviceName, otelhttp.WithFilter(func(r *http.Request) bool {{
\t\treturn r.URL.Path != "/metrics" && r.URL.Path != "/health"
\t}}))
\tlog.Printf("%s listening on %s", serviceName, listenAddr)
\tlog.Fatal(http.ListenAndServe(listenAddr, handler))
}}
'''


# -- Service-specific Go code generators ------------------------------------

def gen_auth_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("auth-service", 8084, imports)
    code += go_prom_and_middleware()
    code += '''
var authOutcomes = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "auth_outcomes_total", Help: "Auth outcomes"},
\t[]string{"outcome"},
)

func init() {
\tprometheus.MustRegister(authOutcomes)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("auth-service")
    code += '''
var sessionServiceURL string
var userServiceURL string

func loginHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" {
\t\tuserID = "user-1"
\t}
\t// Create session
\tsessData, err := httpPost(sessionServiceURL + "/sessions/create?user_id=" + url.QueryEscape(userID))
\tif err != nil {
\t\thttp.Error(w, "Session service unavailable", http.StatusBadGateway)
\t\treturn
\t}
\t// Fetch user
\tuserData, _ := httpGet(userServiceURL + "/users/" + url.QueryEscape(userID))
\tif userData == nil {
\t\tuserData = map[string]interface{}{"user_id": userID}
\t}
\t// Kafka audit event
\tkafkaSend("regression-lab-audit-events", map[string]interface{}{
\t\t"event": "login", "user_id": userID, "ts": float64(time.Now().UnixMilli()) / 1000,
\t})
\tauthOutcomes.WithLabelValues("success").Inc()
\ttoken := "tok-123"
\tif sid, ok := sessData["session_id"]; ok {
\t\ttoken = fmt.Sprintf("%v", sid)
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"token": token, "user": userData})
}

func validateHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\ttoken := r.URL.Query().Get("token")
\tif token == "" {
\t\ttoken = "tok-123"
\t}
\tresult, err := httpGet(sessionServiceURL + "/sessions/validate?token=" + url.QueryEscape(token))
\tif err != nil {
\t\thttp.Error(w, "Invalid token", http.StatusUnauthorized)
\t\treturn
\t}
\tjsonResponse(w, http.StatusOK, result)
}
'''
    code += go_main_func(
        [
            'sessionServiceURL = getEnv("SESSION_SERVICE_URL", "http://session-service:8106")',
            'userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/auth/login", metricsMiddleware(loginHandler))',
            'mux.HandleFunc("/auth/validate", metricsMiddleware(validateHandler))',
        ],
    )
    return code


def gen_orders_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("orders-service", 8085, imports)
    code += go_prom_and_middleware()
    code += '''
var ordersProcessed = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "orders_processed_total", Help: "Orders processed"},
\t[]string{"source"},
)

func init() {
\tprometheus.MustRegister(ordersProcessed)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("orders-service")
    code += '''
var inventoryServiceURL string
var notificationServiceURL string
var shippingServiceURL string

func getOrderHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\torderID := strings.TrimPrefix(r.URL.Path, "/orders/")
\tif orderID == "" || orderID == r.URL.Path {
\t\thttp.Error(w, "order_id required", http.StatusBadRequest)
\t\treturn
\t}
\tordersProcessed.WithLabelValues("http").Inc()
\tinvData, _ := httpGet(inventoryServiceURL + "/inventory/status?order_id=" + url.QueryEscape(orderID))
\tif invData == nil {
\t\tinvData = map[string]interface{}{"status": "unknown"}
\t}
\t// Fire-and-forget: notify about order access
\tgo func() {
\t\t_, err := httpPost(notificationServiceURL + "/notify?user_id=user-1&message=" + url.QueryEscape("Order "+orderID+" accessed"))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: notification-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: check shipping status
\tgo func() {
\t\t_, err := httpGet(shippingServiceURL + "/shipping/status?order_id=" + url.QueryEscape(orderID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: shipping-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"order_id": orderID, "status": "confirmed", "inventory": invData,
\t})
}

func listOrdersHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" {
\t\tuserID = "user-1"
\t}
\torders := make([]map[string]string, 0, 5)
\tfor i := 1; i <= 5; i++ {
\t\torders = append(orders, map[string]string{"id": fmt.Sprintf("ord-%d", i), "status": "completed"})
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"user_id": userID, "orders": orders})
}

func ordersRouter(w http.ResponseWriter, r *http.Request) {
\tpath := r.URL.Path
\tif path == "/orders" || path == "/orders/" {
\t\tlistOrdersHandler(w, r)
\t\treturn
\t}
\tif strings.HasPrefix(path, "/orders/") {
\t\tgetOrderHandler(w, r)
\t\treturn
\t}
\thttp.NotFound(w, r)
}
'''
    # Kafka consumer body for "regression-lab-orders" topic
    consumer_body = '''\t\t\tlog.Printf("Processing order from Kafka: %v", data["checkout_id"])
\t\t\tordersProcessed.WithLabelValues("kafka").Inc()
\t\t\t// Call inventory to reserve
\t\t\tcheckoutID, _ := data["checkout_id"].(string)
\t\t\tif checkoutID == "" { checkoutID = "unknown" }
\t\t\thttpPost(inventoryServiceURL + "/inventory/reserve?order_id=" + url.QueryEscape(checkoutID) + "&items=item-1,item-2")
\t\t\t// Produce inventory-updates
\t\t\tkafkaSend("regression-lab-inventory-updates", map[string]interface{}{
\t\t\t\t"order_id": checkoutID, "status": "reserved",
\t\t\t\t"ts": float64(time.Now().UnixMilli()) / 1000,
\t\t\t})'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_ORDERS, "orders-service-group", consumer_body)
    code += go_main_func(
        [
            'inventoryServiceURL = getEnv("INVENTORY_SERVICE_URL", "http://inventory-service:8086")',
            'notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://notification-service:8100")',
            'shippingServiceURL = getEnv("SHIPPING_SERVICE_URL", "http://shipping-service:8087")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
            'go consumeKafka(kafkaBrokers, "regression-lab-orders", "orders-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/orders/", metricsMiddleware(ordersRouter))',
            'mux.HandleFunc("/orders", metricsMiddleware(ordersRouter))',
        ],
    )
    return code


def gen_inventory_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "sync/atomic", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("inventory-service", 8086, imports)
    code += go_prom_and_middleware()
    code += '''
var inventoryOps = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "inventory_operations_total", Help: "Inventory operations"},
\t[]string{"operation"},
)

func init() {
\tprometheus.MustRegister(inventoryOps)
}
'''
    code += go_json_helpers()
    code += go_fault_injection_inventory()
    code += go_redis_setup()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("inventory-service")
    code += '''
var warehouseServiceURL string
var notificationServiceURL string
var ctx = context.Background()

func reserveHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\torderID := r.URL.Query().Get("order_id")
\titems := r.URL.Query().Get("items")
\tif items == "" { items = "item-1" }
\tinventoryOps.WithLabelValues("reserve").Inc()
\tif redisClient != nil {
\t\tdata, _ := json.Marshal(map[string]interface{}{"items": strings.Split(items, ","), "status": "reserved"})
\t\tredisClient.Set(ctx, "inv:"+orderID, string(data), time.Hour).Err()
\t}
\twhData, _ := httpGet(warehouseServiceURL + "/warehouse/check?items=" + url.QueryEscape(items))
\tif whData == nil { whData = map[string]interface{}{"available": true} }
\t// Fire-and-forget: low stock alert
\tgo func() {
\t\t_, err := httpPost(notificationServiceURL + "/notify?user_id=warehouse-ops&message=" + url.QueryEscape("Inventory reserved for order "+orderID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: notification-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"order_id": orderID, "status": "reserved", "warehouse": whData,
\t})
}

func inventoryStatusHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\torderID := r.URL.Query().Get("order_id")
\tif orderID == "" { orderID = "unknown" }
\tif redisClient != nil {
\t\tval, err := redisClient.Get(ctx, "inv:"+orderID).Result()
\t\tif err == nil {
\t\t\tvar data map[string]interface{}
\t\t\tjson.Unmarshal([]byte(val), &data)
\t\t\tjsonResponse(w, http.StatusOK, data)
\t\t\treturn
\t\t}
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"order_id": orderID, "status": "unknown"})
}
'''
    consumer_body = '''\t\t\tif atomic.LoadInt32(&consumerPaused) != 0 {
\t\t\t\ttime.Sleep(500 * time.Millisecond)
\t\t\t\tcontinue
\t\t\t}
\t\t\tlog.Printf("Inventory update: %v", data["order_id"])
\t\t\tinventoryOps.WithLabelValues("kafka_update").Inc()
\t\t\torderID, _ := data["order_id"].(string)
\t\t\tkafkaSend("regression-lab-shipping-events", map[string]interface{}{
\t\t\t\t"order_id": orderID, "action": "ship",
\t\t\t\t"ts": float64(time.Now().UnixMilli()) / 1000,
\t\t\t})'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_INVENTORY_UPDATES, "inventory-service-group", consumer_body)
    code += go_main_func(
        [
            'warehouseServiceURL = getEnv("WAREHOUSE_SERVICE_URL", "http://warehouse-service:8113")',
            'notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://notification-service:8100")',
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/1")',
            'initRedis(redisURL)',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
            'go consumeKafka(kafkaBrokers, "regression-lab-inventory-updates", "inventory-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/inventory/reserve", metricsMiddleware(reserveHandler))',
            'mux.HandleFunc("/inventory/status", metricsMiddleware(inventoryStatusHandler))',
        ],
    )
    return code


def gen_shipping_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("shipping-service", 8087, imports)
    code += go_prom_and_middleware()
    code += '''
var shipments = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "shipments_total", Help: "Shipments processed"},
\t[]string{"status"},
)

func init() {
\tprometheus.MustRegister(shipments)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("shipping-service")
    code += '''
var warehouseServiceURL string
var notificationServiceURL string
var emailServiceURL string
var analyticsServiceURL string
'''
    # Need a no-op kafkaSend and kafkaProducer since consumer_body might reference them indirectly
    # Actually shipping-service only consumes, doesn't produce
    consumer_body = '''\t\t\torderID, _ := data["order_id"].(string)
\t\t\tif orderID == "" { orderID = "unknown" }
\t\t\tlog.Printf("Shipping event for order %s", orderID)
\t\t\tshipments.WithLabelValues("processing").Inc()
\t\t\thttpPost(warehouseServiceURL + "/warehouse/dispatch?order_id=" + url.QueryEscape(orderID))
\t\t\thttpPost(notificationServiceURL + "/notify?user_id=user-1&message=" + url.QueryEscape("Order "+orderID+" shipped"))
\t\t\tshipments.WithLabelValues("shipped").Inc()'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_SHIPPING_EVENTS, "shipping-service-group", consumer_body)
    code += '''
func getShippingHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\torderID := strings.TrimPrefix(r.URL.Path, "/shipping/")
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"order_id": orderID, "status": "in_transit", "eta": "2d",
\t})
}

func shippingStatusHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\torderID := r.URL.Query().Get("order_id")
\tif orderID == "" { orderID = "unknown" }
\tshipments.WithLabelValues("status_check").Inc()
\t// Fire-and-forget: send shipping confirmation email
\tgo func() {
\t\t_, err := httpPost(emailServiceURL + "/email/send?to=customer@example.com&subject=" + url.QueryEscape("Shipping update for "+orderID) + "&body=" + url.QueryEscape("Your order "+orderID+" is being shipped"))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: email-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: track shipment analytics
\tgo func() {
\t\t_, err := httpPost(analyticsServiceURL + "/analytics/signals?user_id=system&event=shipment_status_check&order_id=" + url.QueryEscape(orderID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: analytics-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"order_id": orderID, "status": "in_transit", "carrier": "FastShip", "eta": "2d",
\t})
}
'''
    code += go_main_func(
        [
            'warehouseServiceURL = getEnv("WAREHOUSE_SERVICE_URL", "http://warehouse-service:8113")',
            'notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://notification-service:8100")',
            'emailServiceURL = getEnv("EMAIL_SERVICE_URL", "http://email-service:8108")',
            'analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go consumeKafka(kafkaBrokers, "regression-lab-shipping-events", "shipping-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/shipping/status", metricsMiddleware(shippingStatusHandler))',
            'mux.HandleFunc("/shipping/", metricsMiddleware(getShippingHandler))',
        ],
    )
    return code


def gen_search_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("search-service", 8088, imports)
    code += go_prom_and_middleware()
    code += '''
var searchCount = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "search_queries_total", Help: "Total search queries"},
\t[]string{"status"},
)

func init() {
\tprometheus.MustRegister(searchCount)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("search-service")
    code += '''
var rankingServiceURL string
var cacheServiceURL string
var analyticsServiceURL string

func searchHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tq := r.URL.Query().Get("q")
\tif q == "" { q = "product" }
\tpage := r.URL.Query().Get("page")
\tif page == "" { page = "1" }
\t// Check cache
\tcached, err := httpGet(cacheServiceURL + "/cache/get?key=" + url.QueryEscape("search:"+q+":"+page))
\tif err == nil {
\t\tif hit, ok := cached["hit"].(bool); ok && hit {
\t\t\tsearchCount.WithLabelValues("cache_hit").Inc()
\t\t\tjsonResponse(w, http.StatusOK, cached["data"])
\t\t\treturn
\t\t}
\t}
\t// Call ranking service
\tranked, err := httpGet(rankingServiceURL + "/rank?q=" + url.QueryEscape(q) + "&page=" + page)
\tresults := make([]interface{}, 0)
\tif err == nil {
\t\tif r, ok := ranked["results"].([]interface{}); ok {
\t\t\tresults = r
\t\t}
\t} else {
\t\tfor i := 1; i <= 10; i++ {
\t\t\tresults = append(results, map[string]interface{}{"id": i, "name": fmt.Sprintf("Product %d", i)})
\t\t}
\t}
\tkafkaSend("regression-lab-analytics-events", map[string]interface{}{
\t\t"event": "search", "query": q, "ts": float64(time.Now().UnixMilli()) / 1000,
\t})
\t// Fire-and-forget: track search query in analytics
\tgo func() {
\t\t_, err := httpPost(analyticsServiceURL + "/analytics/signals?user_id=user-1&event=search_query&query=" + url.QueryEscape(q))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: analytics-service call failed: %v", err)
\t\t}
\t}()
\tsearchCount.WithLabelValues("success").Inc()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"query": q, "page": page, "results": results})
}
'''
    code += go_main_func(
        [
            'rankingServiceURL = getEnv("RANKING_SERVICE_URL", "http://ranking-service:8089")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/search", metricsMiddleware(searchHandler))',
        ],
    )
    return code


def gen_ranking_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("ranking-service", 8089, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("ranking-service")
    code += '''
var profileServiceURL string
var analyticsServiceURL string
var recommendationServiceURL string
var ctx = context.Background()

func rankHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tq := r.URL.Query().Get("q")
\tif q == "" { q = "product" }
\tpage := r.URL.Query().Get("page")
\tif page == "" { page = "1" }
\tpageNum := 1
\tfmt.Sscanf(page, "%d", &pageNum)
\t// Get user profile for personalization
\tprefs, _ := httpGet(profileServiceURL + "/profile/preferences?user_id=user-1")
\tif prefs == nil { prefs = map[string]interface{}{"category": "general"} }
\t// Simulate ranking
\ttime.Sleep(time.Duration(2+rand.Intn(13)) * time.Millisecond)
\tresults := make([]map[string]interface{}, 0, 10)
\tfor i := (pageNum-1)*10 + 1; i <= pageNum*10; i++ {
\t\tresults = append(results, map[string]interface{}{
\t\t\t"id": i, "name": fmt.Sprintf("Product %d", i),
\t\t\t"score": float64(int(rand.Float64()*500+500)) / 1000.0,
\t\t})
\t}
\tif redisClient != nil {
\t\tdata, _ := json.Marshal(results)
\t\tredisClient.Set(ctx, fmt.Sprintf("rank:%s:%s", q, page), string(data), 60*time.Second).Err()
\t}
\t// Fire-and-forget: track ranking in analytics
\tgo func() {
\t\t_, err := httpPost(analyticsServiceURL + "/analytics/signals?user_id=user-1&event=ranking&query=" + url.QueryEscape(q))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: analytics-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: get personalized boost from recommendations
\tgo func() {
\t\t_, err := httpGet(recommendationServiceURL + "/recommendations/user-1")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: recommendation-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"query": q, "page": pageNum, "results": results, "personalized": prefs != nil,
\t})
}
'''
    code += go_main_func(
        [
            'profileServiceURL = getEnv("PROFILE_SERVICE_URL", "http://profile-service:8090")',
            'analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")',
            'recommendationServiceURL = getEnv("RECOMMENDATION_SERVICE_URL", "http://recommendation-service:8095")',
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/2")',
            'initRedis(redisURL)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/rank", metricsMiddleware(rankHandler))',
        ],
    )
    return code


def gen_profile_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("profile-service", 8090, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("profile-service")
    code += '''
var userServiceURL string
var mediaServiceURL string
var loyaltyServiceURL string
var cacheServiceURL string

func getProfileHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := strings.TrimPrefix(r.URL.Path, "/profile/")
\tif userID == "" || userID == "preferences" {
\t\tpreferencesHandler(w, r)
\t\treturn
\t}
\tuserData, _ := httpGet(userServiceURL + "/users/" + url.QueryEscape(userID))
\tif userData == nil { userData = map[string]interface{}{"user_id": userID, "name": "Unknown"} }
\tavatarData, _ := httpGet(mediaServiceURL + "/media/avatar?user_id=" + url.QueryEscape(userID))
\tif avatarData == nil { avatarData = map[string]interface{}{"url": "/default-avatar.png"} }
\t// Fire-and-forget: get loyalty tier for profile
\tgo func() {
\t\t_, err := httpGet(loyaltyServiceURL + "/loyalty/tier?user_id=" + url.QueryEscape(userID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: loyalty-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: cache profile data
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("profile:"+userID) + "&value=" + url.QueryEscape(userID) + "&ttl=300")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "user": userData, "avatar": avatarData,
\t})
}

func preferencesHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "category": "electronics", "price_range": "mid",
\t})
}

func profileRouter(w http.ResponseWriter, r *http.Request) {
\tpath := r.URL.Path
\tif path == "/profile/preferences" {
\t\tpreferencesHandler(w, r)
\t\treturn
\t}
\tif strings.HasPrefix(path, "/profile/") {
\t\tgetProfileHandler(w, r)
\t\treturn
\t}
\thttp.NotFound(w, r)
}
'''
    code += go_main_func(
        [
            'userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")',
            'mediaServiceURL = getEnv("MEDIA_SERVICE_URL", "http://media-service:8114")',
            'loyaltyServiceURL = getEnv("LOYALTY_SERVICE_URL", "http://loyalty-service:8104")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/profile/", metricsMiddleware(profileRouter))',
        ],
    )
    return code


def gen_billing_service():
    imports = [
        "encoding/json", "fmt", "log", "math", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("billing-service", 8091, imports)
    code += go_prom_and_middleware()
    code += '''
var billingOutcomes = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "billing_outcomes_total", Help: "Billing outcomes"},
\t[]string{"outcome"},
)

func init() {
\tprometheus.MustRegister(billingOutcomes)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("billing-service")
    code += '''
var paymentAdapterURL string
var taxServiceURL string
var fraudDetectionURL string
var notificationServiceURL string

func chargeHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tcheckoutID := r.URL.Query().Get("checkout_id")
\tamountStr := r.URL.Query().Get("amount")
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tamount := 100.0
\tfmt.Sscanf(amountStr, "%f", &amount)

\tinvoiceID := fmt.Sprintf("inv-%d", time.Now().UnixNano()%100000000)

\t// Fraud check
\tfraudResult, err := httpPost(fraudDetectionURL + "/fraud/check?user_id=" + url.QueryEscape(userID) + "&amount=" + fmt.Sprintf("%.2f", amount))
\tif err == nil {
\t\tif risk, ok := fraudResult["risk"].(string); ok && risk == "high" {
\t\t\tbillingOutcomes.WithLabelValues("fraud_blocked").Inc()
\t\t\thttp.Error(w, "Transaction blocked by fraud detection", http.StatusForbidden)
\t\t\treturn
\t\t}
\t}

\t// Calculate tax
\ttotal := amount
\ttaxData, err := httpGet(taxServiceURL + "/tax/calculate?amount=" + fmt.Sprintf("%.2f", amount))
\tif err == nil {
\t\tif tax, ok := taxData["tax"].(float64); ok {
\t\t\ttotal = amount + tax
\t\t}
\t} else {
\t\ttotal = math.Round(amount*1.08*100) / 100
\t}

\t// Process via payment adapter
\tpayData, err := httpPost(paymentAdapterURL + "/pay?invoice_id=" + url.QueryEscape(invoiceID) + "&amount=" + fmt.Sprintf("%.2f", total))
\tif err != nil {
\t\tbillingOutcomes.WithLabelValues("payment_failed").Inc()
\t\thttp.Error(w, "Payment adapter failed", http.StatusBadGateway)
\t\treturn
\t}

\tkafkaSend("regression-lab-audit-events", map[string]interface{}{
\t\t"event": "billing", "invoice_id": invoiceID,
\t\t"amount": total, "ts": float64(time.Now().UnixMilli()) / 1000,
\t})
\t// Fire-and-forget: send billing confirmation notification
\tgo func() {
\t\t_, err := httpPost(notificationServiceURL + "/notify?user_id=" + url.QueryEscape(userID) + "&message=" + url.QueryEscape("Billing confirmed: "+invoiceID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: notification-service call failed: %v", err)
\t\t}
\t}()
\tbillingOutcomes.WithLabelValues("success").Inc()
\t_ = checkoutID
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"invoice_id": invoiceID, "amount": total, "payment": payData,
\t})
}
'''
    code += go_main_func(
        [
            'paymentAdapterURL = getEnv("PAYMENT_ADAPTER_URL", "http://payment-adapter:8092")',
            'taxServiceURL = getEnv("TAX_SERVICE_URL", "http://tax-service:8112")',
            'fraudDetectionURL = getEnv("FRAUD_DETECTION_URL", "http://fraud-detection:8111")',
            'notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://notification-service:8100")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/billing/charge", metricsMiddleware(chargeHandler))',
        ],
    )
    return code


def gen_payment_adapter():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("payment-adapter", 8092, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("payment-adapter")
    code += '''
var externalPaymentURL string
var notificationServiceURL string

func payHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tinvoiceID := r.URL.Query().Get("invoice_id")
\tamount := r.URL.Query().Get("amount")
\tresult, err := httpPost(externalPaymentURL + "/external/charge?invoice_id=" + url.QueryEscape(invoiceID) + "&amount=" + amount)
\tif err != nil {
\t\tlog.Printf("External payment error: %v", err)
\t\thttp.Error(w, "External payment failed", http.StatusBadGateway)
\t\treturn
\t}
\t// Fire-and-forget: send payment event notification
\tgo func() {
\t\t_, err := httpPost(notificationServiceURL + "/notify?user_id=payments-ops&message=" + url.QueryEscape("Payment processed: "+invoiceID+" amount="+amount))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: notification-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, result)
}
'''
    code += go_main_func(
        [
            'externalPaymentURL = getEnv("EXTERNAL_PAYMENT_URL", "http://external-payment-api:8115")',
            'notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://notification-service:8100")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/pay", metricsMiddleware(payHandler))',
        ],
    )
    return code


def gen_ingest_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("ingest-service", 8093, imports)
    code += go_prom_and_middleware()
    code += '''
var eventsIngested = prometheus.NewCounter(
\tprometheus.CounterOpts{Name: "events_ingested_total", Help: "Events ingested"},
)

func init() {
\tprometheus.MustRegister(eventsIngested)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("ingest-service")
    code += '''
func produceSynthetic() {
\ttypes := []string{"click", "view", "purchase", "scroll"}
\tfor {
\t\tif kafkaProducer != nil {
\t\t\tkafkaSend("regression-lab-ingest-data", map[string]interface{}{
\t\t\t\t"type":    types[rand.Intn(len(types))],
\t\t\t\t"user_id": fmt.Sprintf("user-%d", rand.Intn(1000)+1),
\t\t\t\t"ts":      float64(time.Now().UnixMilli()) / 1000,
\t\t\t})
\t\t\teventsIngested.Inc()
\t\t}
\t\ttime.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
\t}
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\teventType := r.URL.Query().Get("event_type")
\tif eventType == "" { eventType = "click" }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tkafkaSend("regression-lab-ingest-data", map[string]interface{}{
\t\t"type": eventType, "user_id": userID,
\t\t"ts": float64(time.Now().UnixMilli()) / 1000,
\t})
\teventsIngested.Inc()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"status": "ingested", "type": eventType})
}
'''
    code += go_main_func(
        [
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
            'go produceSynthetic()',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/ingest", metricsMiddleware(ingestHandler))',
        ],
    )
    return code


def gen_processing_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("processing-service", 8094, imports)
    code += go_prom_and_middleware()
    code += '''
var eventsProcessed = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "events_processed_total", Help: "Events processed"},
\t[]string{"type"},
)

func init() {
\tprometheus.MustRegister(eventsProcessed)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("processing-service")
    consumer_body = '''\t\t\teventType, _ := data["type"].(string)
\t\t\tif eventType == "" { eventType = "unknown" }
\t\t\teventsProcessed.WithLabelValues(eventType).Inc()
\t\t\tif (eventType == "purchase" || eventType == "click") && kafkaProducer != nil {
\t\t\t\tkafkaSend("regression-lab-recommendations", map[string]interface{}{
\t\t\t\t\t"user_id":  data["user_id"],
\t\t\t\t\t"based_on": eventType,
\t\t\t\t\t"ts":       float64(time.Now().UnixMilli()) / 1000,
\t\t\t\t})
\t\t\t}'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_INGEST_DATA, "processing-service-group", consumer_body)
    code += '''
func statsHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"status": "running", "pipeline": "ingest-data -> recommendations",
\t})
}
'''
    code += go_main_func(
        [
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
            'go consumeKafka(kafkaBrokers, "regression-lab-ingest-data", "processing-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/processing/stats", metricsMiddleware(statsHandler))',
        ],
    )
    return code


def gen_recommendation_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("recommendation-service", 8095, imports)
    code += go_prom_and_middleware()
    code += '''
var recsServed = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "recommendations_served_total", Help: "Recommendations served"},
\t[]string{"source"},
)

func init() {
\tprometheus.MustRegister(recsServed)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("recommendation-service")
    code += '''
var analyticsServiceURL string
var deliveryServiceURL string
var userServiceURL string
var cacheServiceURL string
var ctx = context.Background()
'''
    # Consumer body for "regression-lab-recommendations" topic
    consumer_body = '''\t\t\tuserID, _ := data["user_id"].(string)
\t\t\tif userID == "" { userID = "unknown" }
\t\t\trecsServed.WithLabelValues("kafka").Inc()
\t\t\titems := make([]string, 5)
\t\t\tfor i := range items { items[i] = fmt.Sprintf("rec-%d", rand.Intn(100)+1) }
\t\t\tif redisClient != nil {
\t\t\t\tdata, _ := json.Marshal(map[string]interface{}{"items": items})
\t\t\t\tredisClient.Set(ctx, "rec:"+userID, string(data), 5*time.Minute).Err()
\t\t\t}
\t\t\thttpPost(deliveryServiceURL + "/deliver?user_id=" + url.QueryEscape(userID) + "&type=recommendation")'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_RECOMMENDATIONS, "recommendation-service-group", consumer_body)
    code += '''
func getRecommendationsHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := strings.TrimPrefix(r.URL.Path, "/recommendations/")
\tif userID == "" { userID = "user-1" }
\t// Check Redis cache
\tif redisClient != nil {
\t\tval, err := redisClient.Get(ctx, "rec:"+userID).Result()
\t\tif err == nil {
\t\t\trecsServed.WithLabelValues("cache").Inc()
\t\t\tvar cached map[string]interface{}
\t\t\tjson.Unmarshal([]byte(val), &cached)
\t\t\tjsonResponse(w, http.StatusOK, cached)
\t\t\treturn
\t\t}
\t}
\t// Call analytics for user signals
\tsignals, _ := httpGet(analyticsServiceURL + "/analytics/signals?user_id=" + url.QueryEscape(userID))
\tif signals == nil { signals = map[string]interface{}{} }
\t// Fire-and-forget: get user personalization signals
\tgo func() {
\t\t_, err := httpGet(userServiceURL + "/users/" + url.QueryEscape(userID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: user-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: cache recommendation results
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("recs:"+userID) + "&value=" + url.QueryEscape(userID) + "&ttl=300")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\trecs := make([]map[string]interface{}, 10)
\tfor i := range recs {
\t\trecs[i] = map[string]interface{}{
\t\t\t"id": fmt.Sprintf("rec-%d", rand.Intn(100)+1),
\t\t\t"score": float64(int(rand.Float64()*500+500)) / 1000.0,
\t\t}
\t}
\trecsServed.WithLabelValues("computed").Inc()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "recommendations": recs, "signals": signals,
\t})
}
'''
    code += go_main_func(
        [
            'analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")',
            'deliveryServiceURL = getEnv("DELIVERY_SERVICE_URL", "http://delivery-service:8096")',
            'userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/3")',
            'initRedis(redisURL)',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go consumeKafka(kafkaBrokers, "regression-lab-recommendations", "recommendation-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/recommendations/", metricsMiddleware(getRecommendationsHandler))',
        ],
    )
    return code


def gen_delivery_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("delivery-service", 8096, imports)
    code += go_prom_and_middleware()
    code += '''
var deliveries = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "deliveries_total", Help: "Content deliveries"},
\t[]string{"type"},
)

func init() {
\tprometheus.MustRegister(deliveries)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("delivery-service")
    code += '''
var notificationServiceURL string
var warehouseServiceURL string
var emailServiceURL string

func deliverHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tdType := r.URL.Query().Get("type")
\tif dType == "" { dType = "recommendation" }
\tdeliveries.WithLabelValues(dType).Inc()
\thttpPost(notificationServiceURL + "/notify?user_id=" + url.QueryEscape(userID) + "&message=" + url.QueryEscape("New "+dType+" available"))
\t// Fire-and-forget: check warehouse availability
\tgo func() {
\t\t_, err := httpGet(warehouseServiceURL + "/warehouse/check?items=item-1")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: warehouse-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: send delivery notification email
\tgo func() {
\t\t_, err := httpPost(emailServiceURL + "/email/send?to=" + url.QueryEscape(userID+"@example.com") + "&subject=" + url.QueryEscape("Delivery notification") + "&body=" + url.QueryEscape("New "+dType+" delivered for "+userID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: email-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"status": "delivered", "user_id": userID, "type": dType,
\t})
}
'''
    code += go_main_func(
        [
            'notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://notification-service:8100")',
            'warehouseServiceURL = getEnv("WAREHOUSE_SERVICE_URL", "http://warehouse-service:8113")',
            'emailServiceURL = getEnv("EMAIL_SERVICE_URL", "http://email-service:8108")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/deliver", metricsMiddleware(deliverHandler))',
        ],
    )
    return code


def gen_pricing_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("pricing-service", 8097, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("pricing-service")
    code += '''
var discountServiceURL string
var taxServiceURL string
var cacheServiceURL string
var ctx = context.Background()

func calculateHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\titems := r.URL.Query().Get("items")
\tif items == "" { items = "item-1,item-2" }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\titemList := strings.Split(items, ",")
\tbaseTotal := 0.0
\tfor range itemList {
\t\tbaseTotal += float64(int(rand.Float64()*9000+1000)) / 100.0
\t}
\t// Get discount
\tfinalTotal := baseTotal
\tdiscData, err := httpGet(discountServiceURL + "/discount/apply?user_id=" + userID + "&amount=" + fmt.Sprintf("%.2f", baseTotal))
\tif err == nil {
\t\tif fa, ok := discData["final_amount"].(float64); ok {
\t\t\tfinalTotal = fa
\t\t}
\t}
\tif redisClient != nil {
\t\tdata, _ := json.Marshal(map[string]interface{}{"base": baseTotal, "final": finalTotal})
\t\tredisClient.Set(ctx, fmt.Sprintf("price:%s:%s", userID, items), string(data), 2*time.Minute).Err()
\t}
\t// Fire-and-forget: pre-calculate tax
\tgo func() {
\t\t_, err := httpGet(taxServiceURL + "/tax/calculate?amount=" + fmt.Sprintf("%.2f", finalTotal))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: tax-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: cache pricing data
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape(fmt.Sprintf("pricing:%s:%s", userID, items)) + "&value=" + url.QueryEscape(fmt.Sprintf("%.2f", finalTotal)) + "&ttl=120")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"items": itemList, "base_total": baseTotal, "final_total": finalTotal,
\t})
}
'''
    code += go_main_func(
        [
            'discountServiceURL = getEnv("DISCOUNT_SERVICE_URL", "http://discount-service:8098")',
            'taxServiceURL = getEnv("TAX_SERVICE_URL", "http://tax-service:8112")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/4")',
            'initRedis(redisURL)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/pricing/calculate", metricsMiddleware(calculateHandler))',
        ],
    )
    return code


def gen_discount_service():
    imports = [
        "encoding/json", "fmt", "log", "math", "math/rand", "net/http",
        "net/url", "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("discount-service", 8098, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("discount-service")
    code += '''
var loyaltyServiceURL string
var cacheServiceURL string

func applyDiscountHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tamount := 100.0
\tfmt.Sscanf(r.URL.Query().Get("amount"), "%f", &amount)
\t// Check loyalty tier
\ttier := "bronze"
\tloyaltyData, err := httpGet(loyaltyServiceURL + "/loyalty/tier?user_id=" + userID)
\tif err == nil {
\t\tif t, ok := loyaltyData["tier"].(string); ok {
\t\t\ttier = t
\t\t}
\t}
\ttierDiscounts := map[string]float64{"bronze": 0.0, "silver": 0.05, "gold": 0.10, "platinum": 0.15}
\tpct := tierDiscounts[tier]
\tdiscount := math.Round(amount*pct*100) / 100
\tfinalAmt := math.Round((amount-discount)*100) / 100
\t// Fire-and-forget: cache discount calculation
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("discount:"+userID) + "&value=" + url.QueryEscape(fmt.Sprintf("%.2f", finalAmt)) + "&ttl=300")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"original": amount, "discount": discount, "final_amount": finalAmt, "tier": tier,
\t})
}
'''
    code += go_main_func(
        [
            'loyaltyServiceURL = getEnv("LOYALTY_SERVICE_URL", "http://loyalty-service:8104")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/discount/apply", metricsMiddleware(applyDiscountHandler))',
        ],
    )
    return code


def gen_user_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("user-service", 8099, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("user-service")
    code += '''
var cacheServiceURL string
var users map[string]map[string]interface{}

func initUsers() {
\ttiers := []string{"bronze", "silver", "gold", "platinum"}
\tusers = make(map[string]map[string]interface{}, 100)
\tfor i := 1; i <= 100; i++ {
\t\tuid := fmt.Sprintf("user-%d", i)
\t\tusers[uid] = map[string]interface{}{
\t\t\t"user_id": uid,
\t\t\t"name":    fmt.Sprintf("User %d", i),
\t\t\t"email":   fmt.Sprintf("user%d@example.com", i),
\t\t\t"tier":    tiers[rand.Intn(len(tiers))],
\t\t}
\t}
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := strings.TrimPrefix(r.URL.Path, "/users/")
\tvar resp map[string]interface{}
\tif user, ok := users[userID]; ok {
\t\tresp = user
\t} else {
\t\tresp = map[string]interface{}{
\t\t\t"user_id": userID, "name": "Unknown", "email": "unknown@example.com",
\t\t}
\t}
\t// Fire-and-forget: cache user lookup
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("user:"+userID) + "&value=" + url.QueryEscape(userID) + "&ttl=300")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, resp)
}

func listUsersHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tpage := 1
\tlimit := 10
\tfmt.Sscanf(r.URL.Query().Get("page"), "%d", &page)
\tfmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
\tallUsers := make([]map[string]interface{}, 0, len(users))
\tfor _, u := range users {
\t\tallUsers = append(allUsers, u)
\t}
\tstart := (page - 1) * limit
\tend := start + limit
\tif start > len(allUsers) { start = len(allUsers) }
\tif end > len(allUsers) { end = len(allUsers) }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"page": page, "users": allUsers[start:end],
\t})
}

func usersRouter(w http.ResponseWriter, r *http.Request) {
\tpath := r.URL.Path
\tif path == "/users" || path == "/users/" {
\t\tlistUsersHandler(w, r)
\t\treturn
\t}
\tif strings.HasPrefix(path, "/users/") {
\t\tgetUserHandler(w, r)
\t\treturn
\t}
\thttp.NotFound(w, r)
}
'''
    code += go_main_func(
        [
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'initUsers()',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/users/", metricsMiddleware(usersRouter))',
            'mux.HandleFunc("/users", metricsMiddleware(usersRouter))',
        ],
    )
    return code


def gen_notification_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("notification-service", 8100, imports)
    code += go_prom_and_middleware()
    code += '''
var notifications = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "notifications_total", Help: "Notifications sent"},
\t[]string{"channel"},
)

func init() {
\tprometheus.MustRegister(notifications)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("notification-service")
    code += '''
var emailServiceURL string
'''
    consumer_body = '''\t\t\tnotifications.WithLabelValues("kafka").Inc()
\t\t\temail, _ := data["email"].(string)
\t\t\tif email == "" { email = "user@example.com" }
\t\t\tsubject, _ := data["subject"].(string)
\t\t\tif subject == "" { subject = "Notification" }
\t\t\tbody, _ := data["body"].(string)
\t\t\tif body == "" { body = "You have a notification" }
\t\t\thttpPost(emailServiceURL + "/email/send?to=" + url.QueryEscape(email) + "&subject=" + url.QueryEscape(subject) + "&body=" + url.QueryEscape(body))'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_NOTIFICATIONS, "notification-service-group", consumer_body)
    code += '''
func notifyHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tmessage := r.URL.Query().Get("message")
\tif message == "" { message = "Hello" }
\tnotifications.WithLabelValues("http").Inc()
\thttpPost(emailServiceURL + "/email/send?to=" + url.QueryEscape(userID+"@example.com") + "&subject=Notification&body=" + url.QueryEscape(message))
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"status": "sent", "user_id": userID,
\t})
}
'''
    code += go_main_func(
        [
            'emailServiceURL = getEnv("EMAIL_SERVICE_URL", "http://email-service:8108")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go consumeKafka(kafkaBrokers, "regression-lab-notifications", "notification-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/notify", metricsMiddleware(notifyHandler))',
        ],
    )
    return code


def gen_cart_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("cart-service", 8101, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("cart-service")
    code += '''
var catalogServiceURL string
var pricingServiceURL string
var sessionServiceURL string
var ctx = context.Background()

func getCartHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\t// Extract user_id from /cart/{user_id} (but not /cart/{user_id}/add)
\tpath := strings.TrimPrefix(r.URL.Path, "/cart/")
\tparts := strings.SplitN(path, "/", 2)
\tuserID := parts[0]
\tif redisClient != nil {
\t\tval, err := redisClient.Get(ctx, "cart:"+userID).Result()
\t\tif err == nil {
\t\t\tvar cart map[string]interface{}
\t\t\tjson.Unmarshal([]byte(val), &cart)
\t\t\tjsonResponse(w, http.StatusOK, cart)
\t\t\treturn
\t\t}
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "items": []interface{}{}, "total": 0,
\t})
}

func addToCartHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\t// /cart/{user_id}/add
\tpath := strings.TrimPrefix(r.URL.Path, "/cart/")
\tparts := strings.SplitN(path, "/", 2)
\tuserID := parts[0]
\tproductID := r.URL.Query().Get("product_id")
\tif productID == "" { productID = "prod-1" }
\t// Validate product
\tproduct, _ := httpGet(catalogServiceURL + "/catalog/product/" + url.QueryEscape(productID))
\tif product == nil { product = map[string]interface{}{"id": productID, "price": 29.99} }
\tcart := map[string]interface{}{"user_id": userID, "items": []interface{}{}, "total": 0.0}
\tif redisClient != nil {
\t\tval, err := redisClient.Get(ctx, "cart:"+userID).Result()
\t\tif err == nil {
\t\t\tjson.Unmarshal([]byte(val), &cart)
\t\t}
\t}
\titems, _ := cart["items"].([]interface{})
\titems = append(items, product)
\tcart["items"] = items
\ttotal := 0.0
\tfor _, item := range items {
\t\tif m, ok := item.(map[string]interface{}); ok {
\t\t\tif p, ok := m["price"].(float64); ok { total += p }
\t\t}
\t}
\tcart["total"] = total
\tif redisClient != nil {
\t\tdata, _ := json.Marshal(cart)
\t\tredisClient.Set(ctx, "cart:"+userID, string(data), time.Hour).Err()
\t}
\t// Fire-and-forget: get real-time pricing
\tgo func() {
\t\t_, err := httpGet(pricingServiceURL + "/pricing/calculate?items=" + url.QueryEscape(productID) + "&user_id=" + url.QueryEscape(userID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: pricing-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: validate session
\tgo func() {
\t\t_, err := httpGet(sessionServiceURL + "/sessions/validate?token=" + url.QueryEscape("sess-"+userID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: session-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, cart)
}

func cartRouter(w http.ResponseWriter, r *http.Request) {
\tpath := r.URL.Path
\tif strings.HasSuffix(path, "/add") && r.Method == http.MethodPost {
\t\taddToCartHandler(w, r)
\t\treturn
\t}
\tif strings.HasPrefix(path, "/cart/") {
\t\tgetCartHandler(w, r)
\t\treturn
\t}
\thttp.NotFound(w, r)
}
'''
    code += go_main_func(
        [
            'catalogServiceURL = getEnv("CATALOG_SERVICE_URL", "http://catalog-service:8102")',
            'pricingServiceURL = getEnv("PRICING_SERVICE_URL", "http://pricing-service:8097")',
            'sessionServiceURL = getEnv("SESSION_SERVICE_URL", "http://session-service:8106")',
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/5")',
            'initRedis(redisURL)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/cart/", metricsMiddleware(cartRouter))',
        ],
    )
    return code


def gen_catalog_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("catalog-service", 8102, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("catalog-service")
    code += '''
var reviewServiceURL string
var mediaServiceURL string
var cacheServiceURL string
var pricingServiceURL string
var inventoryServiceURL string

type Product struct {
\tID       string  `json:"id"`
\tName     string  `json:"name"`
\tPrice    float64 `json:"price"`
\tCategory string  `json:"category"`
}

var products []Product

func initProducts() {
\tcategories := []string{"electronics", "clothing", "food", "books"}
\tproducts = make([]Product, 100)
\tfor i := 0; i < 100; i++ {
\t\tproducts[i] = Product{
\t\t\tID:       fmt.Sprintf("prod-%d", i+1),
\t\t\tName:     fmt.Sprintf("Product %d", i+1),
\t\t\tPrice:    float64(int(rand.Float64()*19500+500)) / 100.0,
\t\t\tCategory: categories[rand.Intn(len(categories))],
\t\t}
\t}
}

func listProductsHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tpage := 1
\tlimit := 10
\tfmt.Sscanf(r.URL.Query().Get("page"), "%d", &page)
\tfmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
\tcategory := r.URL.Query().Get("category")
\t// Check cache
\tcached, err := httpGet(cacheServiceURL + "/cache/get?key=" + url.QueryEscape(fmt.Sprintf("catalog:%s:%d", category, page)))
\tif err == nil {
\t\tif hit, ok := cached["hit"].(bool); ok && hit {
\t\t\tjsonResponse(w, http.StatusOK, cached["data"])
\t\t\treturn
\t\t}
\t}
\tfiltered := make([]Product, 0)
\tfor _, p := range products {
\t\tif category == "" || p.Category == category {
\t\t\tfiltered = append(filtered, p)
\t\t}
\t}
\tstart := (page - 1) * limit
\tend := start + limit
\tif start > len(filtered) { start = len(filtered) }
\tif end > len(filtered) { end = len(filtered) }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"page": page, "products": filtered[start:end],
\t})
}

func getProductHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tproductID := strings.TrimPrefix(r.URL.Path, "/catalog/product/")
\tvar found *Product
\tfor i := range products {
\t\tif products[i].ID == productID {
\t\t\tfound = &products[i]
\t\t\tbreak
\t\t}
\t}
\tif found == nil {
\t\thttp.Error(w, "Product not found", http.StatusNotFound)
\t\treturn
\t}
\treviewData, _ := httpGet(reviewServiceURL + "/reviews/" + url.QueryEscape(productID))
\tif reviewData == nil { reviewData = map[string]interface{}{"reviews": []interface{}{}} }
\tmediaData, _ := httpGet(mediaServiceURL + "/media/product/" + url.QueryEscape(productID))
\tif mediaData == nil { mediaData = map[string]interface{}{"images": []interface{}{}} }
\t// Fire-and-forget: get real-time pricing
\tgo func() {
\t\t_, err := httpGet(pricingServiceURL + "/pricing/calculate?items=" + url.QueryEscape(productID) + "&user_id=user-1")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: pricing-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: check inventory stock status
\tgo func() {
\t\t_, err := httpGet(inventoryServiceURL + "/inventory/status?order_id=" + url.QueryEscape(productID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: inventory-service call failed: %v", err)
\t\t}
\t}()
\tresult := map[string]interface{}{
\t\t"id": found.ID, "name": found.Name, "price": found.Price, "category": found.Category,
\t\t"reviews": reviewData["reviews"], "media": mediaData,
\t}
\tjsonResponse(w, http.StatusOK, result)
}

func catalogRouter(w http.ResponseWriter, r *http.Request) {
\tpath := r.URL.Path
\tif path == "/catalog/products" {
\t\tlistProductsHandler(w, r)
\t\treturn
\t}
\tif strings.HasPrefix(path, "/catalog/product/") {
\t\tgetProductHandler(w, r)
\t\treturn
\t}
\thttp.NotFound(w, r)
}
'''
    code += go_main_func(
        [
            'initProducts()',
            'reviewServiceURL = getEnv("REVIEW_SERVICE_URL", "http://review-service:8103")',
            'mediaServiceURL = getEnv("MEDIA_SERVICE_URL", "http://media-service:8114")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'pricingServiceURL = getEnv("PRICING_SERVICE_URL", "http://pricing-service:8097")',
            'inventoryServiceURL = getEnv("INVENTORY_SERVICE_URL", "http://inventory-service:8086")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/catalog/", metricsMiddleware(catalogRouter))',
            'mux.HandleFunc("/catalog/products", metricsMiddleware(listProductsHandler))',
        ],
    )
    return code


def gen_review_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("review-service", 8103, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("review-service")
    code += '''
var recommendationServiceURL string
var userServiceURL string
var mediaServiceURL string

func getReviewsHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tproductID := strings.TrimPrefix(r.URL.Path, "/reviews/")
\tnumReviews := rand.Intn(5) + 3
\treviews := make([]map[string]interface{}, numReviews)
\tfor i := 0; i < numReviews; i++ {
\t\treviews[i] = map[string]interface{}{
\t\t\t"id":         fmt.Sprintf("rev-%d", i+1),
\t\t\t"product_id": productID,
\t\t\t"rating":     rand.Intn(5) + 1,
\t\t\t"text":       fmt.Sprintf("Review %d for %s", i+1, productID),
\t\t\t"user_id":    fmt.Sprintf("user-%d", rand.Intn(50)+1),
\t\t}
\t}
\t// Call recommendation service (continues the deep chain)
\trecData, _ := httpGet(recommendationServiceURL + "/recommendations/user-1")
\trelated := make([]interface{}, 0)
\tif recData != nil {
\t\tif recs, ok := recData["recommendations"].([]interface{}); ok && len(recs) > 3 {
\t\t\trelated = recs[:3]
\t\t} else if recs, ok := recData["recommendations"].([]interface{}); ok {
\t\t\trelated = recs
\t\t}
\t}
\tkafkaSend("regression-lab-analytics-events", map[string]interface{}{
\t\t"event": "review_view", "product_id": productID,
\t\t"ts": float64(time.Now().UnixMilli()) / 1000,
\t})
\t// Fire-and-forget: get reviewer info
\tgo func() {
\t\t_, err := httpGet(userServiceURL + "/users/user-1")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: user-service call failed: %v", err)
\t\t}
\t}()
\t// Fire-and-forget: get review images
\tgo func() {
\t\t_, err := httpGet(mediaServiceURL + "/media/product/" + url.QueryEscape(productID))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: media-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"product_id": productID, "reviews": reviews, "related": related,
\t})
}
'''
    code += go_main_func(
        [
            'recommendationServiceURL = getEnv("RECOMMENDATION_SERVICE_URL", "http://recommendation-service:8095")',
            'userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")',
            'mediaServiceURL = getEnv("MEDIA_SERVICE_URL", "http://media-service:8114")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/reviews/", metricsMiddleware(getReviewsHandler))',
        ],
    )
    return code


def gen_loyalty_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("loyalty-service", 8104, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("loyalty-service")
    code += '''
var userServiceURL string
var cacheServiceURL string
var ctx = context.Background()

func getTierHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tif redisClient != nil {
\t\tval, err := redisClient.Get(ctx, "loyalty:"+userID).Result()
\t\tif err == nil {
\t\t\tvar cached map[string]interface{}
\t\t\tjson.Unmarshal([]byte(val), &cached)
\t\t\tjsonResponse(w, http.StatusOK, cached)
\t\t\treturn
\t\t}
\t}
\ttier := "bronze"
\tuserData, err := httpGet(userServiceURL + "/users/" + userID)
\tif err == nil {
\t\tif t, ok := userData["tier"].(string); ok {
\t\t\ttier = t
\t\t}
\t}
\tpoints := rand.Intn(10001)
\tresult := map[string]interface{}{"user_id": userID, "tier": tier, "points": points}
\tif redisClient != nil {
\t\tdata, _ := json.Marshal(result)
\t\tredisClient.Set(ctx, "loyalty:"+userID, string(data), 5*time.Minute).Err()
\t}
\t// Fire-and-forget: cache loyalty data via cache-service
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("loyalty:"+userID) + "&value=" + url.QueryEscape(fmt.Sprintf("%d", points)) + "&ttl=300")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, result)
}

func getPointsHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "points": rand.Intn(10001),
\t})
}
'''
    code += go_main_func(
        [
            'userServiceURL = getEnv("USER_SERVICE_URL", "http://user-service:8099")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/6")',
            'initRedis(redisURL)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/loyalty/tier", metricsMiddleware(getTierHandler))',
            'mux.HandleFunc("/loyalty/points", metricsMiddleware(getPointsHandler))',
        ],
    )
    return code


def gen_audit_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("audit-service", 8105, imports)
    code += go_prom_and_middleware()
    code += '''
var auditEventsCounter = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "audit_events_total", Help: "Audit events processed"},
\t[]string{"event_type"},
)

func init() {
\tprometheus.MustRegister(auditEventsCounter)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("audit-service")
    code += '''
var (
\tauditLog   []map[string]interface{}
\tauditMutex sync.Mutex
)
'''
    consumer_body = '''\t\t\teventType, _ := data["event"].(string)
\t\t\tif eventType == "" { eventType = "unknown" }
\t\t\tauditEventsCounter.WithLabelValues(eventType).Inc()
\t\t\tauditMutex.Lock()
\t\t\tauditLog = append(auditLog, data)
\t\t\tif len(auditLog) > 10000 {
\t\t\t\tauditLog = auditLog[1:]
\t\t\t}
\t\t\tauditMutex.Unlock()
\t\t\tlog.Printf("Audit event: %s", eventType)'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_AUDIT_EVENTS, "audit-service-group", consumer_body)
    code += '''
func recentHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tlimit := 50
\tfmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
\tauditMutex.Lock()
\tdefer auditMutex.Unlock()
\tstart := len(auditLog) - limit
\tif start < 0 { start = 0 }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"events": auditLog[start:]})
}
'''
    code += go_main_func(
        [
            'auditLog = make([]map[string]interface{}, 0)',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go consumeKafka(kafkaBrokers, "regression-lab-audit-events", "audit-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/audit/recent", metricsMiddleware(recentHandler))',
        ],
    )
    return code


def gen_session_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand", "net/http",
        "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("session-service", 8106, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("session-service")
    code += '''
var ctx = context.Background()

func createSessionHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tsessionID := fmt.Sprintf("sess-%d", time.Now().UnixNano())
\tif redisClient != nil {
\t\tdata, _ := json.Marshal(map[string]interface{}{"user_id": userID, "created": float64(time.Now().Unix())})
\t\tredisClient.Set(ctx, "session:"+sessionID, string(data), time.Hour).Err()
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"session_id": sessionID, "user_id": userID,
\t})
}

func validateSessionHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\ttoken := r.URL.Query().Get("token")
\tif redisClient != nil && token != "" {
\t\tval, err := redisClient.Get(ctx, "session:"+token).Result()
\t\tif err == nil {
\t\t\tvar data map[string]interface{}
\t\t\tjson.Unmarshal([]byte(val), &data)
\t\t\tdata["valid"] = true
\t\t\tjsonResponse(w, http.StatusOK, data)
\t\t\treturn
\t\t}
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"valid": false})
}
'''
    code += go_main_func(
        [
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/7")',
            'initRedis(redisURL)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/sessions/create", metricsMiddleware(createSessionHandler))',
            'mux.HandleFunc("/sessions/validate", metricsMiddleware(validateSessionHandler))',
        ],
    )
    return code


def gen_analytics_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http", "net/url",
        "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("analytics-service", 8107, imports)
    code += go_prom_and_middleware()
    code += '''
var analyticsEvents = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "analytics_events_total", Help: "Analytics events"},
\t[]string{"event_type"},
)

func init() {
\tprometheus.MustRegister(analyticsEvents)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("analytics-service")
    code += '''
var (
\teventCounts map[string]int
\teventMutex  sync.Mutex
)

var reportingServiceURL string
var cacheServiceURL string
'''
    consumer_body = '''\t\t\teventType, _ := data["event"].(string)
\t\t\tif eventType == "" { eventType = "unknown" }
\t\t\tanalyticsEvents.WithLabelValues(eventType).Inc()
\t\t\teventMutex.Lock()
\t\t\teventCounts[eventType]++
\t\t\teventMutex.Unlock()'''
    code += go_kafka_consumer_func(KAFKA_TOPIC_ANALYTICS_EVENTS, "analytics-service-group", consumer_body)
    code += '''
func signalsHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\treportData, _ := httpGet(reportingServiceURL + "/reports/summary")
\tif reportData == nil { reportData = map[string]interface{}{} }
\teventMutex.Lock()
\tcounts := make(map[string]int)
\tfor k, v := range eventCounts { counts[k] = v }
\teventMutex.Unlock()
\t// Fire-and-forget: cache analytics data
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("analytics:"+userID) + "&value=" + url.QueryEscape("signals") + "&ttl=120")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "event_counts": counts, "report": reportData,
\t})
}

func dashboardHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\teventMutex.Lock()
\tcounts := make(map[string]int)
\tfor k, v := range eventCounts { counts[k] = v }
\teventMutex.Unlock()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"event_counts": counts, "status": "active",
\t})
}
'''
    code += go_main_func(
        [
            'eventCounts = make(map[string]int)',
            'reportingServiceURL = getEnv("REPORTING_SERVICE_URL", "http://reporting-service:8110")',
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go consumeKafka(kafkaBrokers, "regression-lab-analytics-events", "analytics-service-group")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/analytics/signals", metricsMiddleware(signalsHandler))',
            'mux.HandleFunc("/analytics/dashboard", metricsMiddleware(dashboardHandler))',
        ],
    )
    return code


def gen_email_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand", "net/http",
        "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("email-service", 8108, imports)
    code += go_prom_and_middleware()
    code += '''
var emailsSent = prometheus.NewCounter(
\tprometheus.CounterOpts{Name: "emails_sent_total", Help: "Emails sent"},
)

func init() {
\tprometheus.MustRegister(emailsSent)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("email-service")
    code += '''
func sendEmailHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tto := r.URL.Query().Get("to")
\tif to == "" { to = "user@example.com" }
\tsubject := r.URL.Query().Get("subject")
\tif subject == "" { subject = "Hello" }
\t// Simulate email sending with small delay
\ttime.Sleep(5 * time.Millisecond)
\temailsSent.Inc()
\tlog.Printf("Email sent to %s: %s", to, subject)
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"status": "sent", "to": to, "subject": subject,
\t})
}
'''
    code += go_main_func(
        [],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/email/send", metricsMiddleware(sendEmailHandler))',
        ],
    )
    return code


def gen_cache_service():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand", "net/http",
        "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("cache-service", 8109, imports)
    code += go_prom_and_middleware()
    code += '''
var cacheOps = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "cache_operations_total", Help: "Cache operations"},
\t[]string{"operation", "result"},
)

func init() {
\tprometheus.MustRegister(cacheOps)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("cache-service")
    code += '''
var ctx = context.Background()

func cacheGetHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tkey := r.URL.Query().Get("key")
\tif redisClient != nil {
\t\tval, err := redisClient.Get(ctx, "cache:"+key).Result()
\t\tif err == nil {
\t\t\tcacheOps.WithLabelValues("get", "hit").Inc()
\t\t\tvar data interface{}
\t\t\tjson.Unmarshal([]byte(val), &data)
\t\t\tjsonResponse(w, http.StatusOK, map[string]interface{}{"hit": true, "data": data})
\t\t\treturn
\t\t}
\t}
\tcacheOps.WithLabelValues("get", "miss").Inc()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"hit": false, "data": nil})
}

func cacheSetHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tkey := r.URL.Query().Get("key")
\tvalue := r.URL.Query().Get("value")
\tif value == "" { value = "{}" }
\tttl := 300
\tfmt.Sscanf(r.URL.Query().Get("ttl"), "%d", &ttl)
\tif redisClient != nil {
\t\terr := redisClient.Set(ctx, "cache:"+key, value, time.Duration(ttl)*time.Second).Err()
\t\tif err != nil {
\t\t\tlog.Printf("Redis error: %v", err)
\t\t\tcacheOps.WithLabelValues("set", "error").Inc()
\t\t} else {
\t\t\tcacheOps.WithLabelValues("set", "ok").Inc()
\t\t}
\t}
\tjsonResponse(w, http.StatusOK, map[string]interface{}{"status": "ok"})
}
'''
    code += go_main_func(
        [
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/8")',
            'initRedis(redisURL)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/cache/get", metricsMiddleware(cacheGetHandler))',
            'mux.HandleFunc("/cache/set", metricsMiddleware(cacheSetHandler))',
        ],
    )
    return code


def gen_reporting_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("reporting-service", 8110, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("reporting-service")
    code += '''
var cacheServiceURL string

func summaryHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\t// Simulate report generation with slight delay
\ttime.Sleep(time.Duration(2+rand.Intn(8)) * time.Millisecond)
\tresult := map[string]interface{}{
\t\t"total_orders": rand.Intn(4001) + 1000,
\t\t"revenue":      float64(int(rand.Float64()*150000+50000)*100) / 100,
\t\t"active_users": rand.Intn(801) + 200,
\t\t"generated_at": float64(time.Now().UnixMilli()) / 1000,
\t}
\t// Fire-and-forget: cache report data
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("report:summary") + "&value=" + url.QueryEscape("report-data") + "&ttl=60")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, result)
}

func detailedHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tperiod := r.URL.Query().Get("period")
\tif period == "" { period = "daily" }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"period":  period,
\t\t"metrics": map[string]interface{}{"orders": 150, "revenue": 12500.0, "returns": 5},
\t})
}
'''
    code += go_main_func(
        [
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/reports/summary", metricsMiddleware(summaryHandler))',
            'mux.HandleFunc("/reports/detailed", metricsMiddleware(detailedHandler))',
        ],
    )
    return code


def gen_fraud_detection():
    imports = [
        "context", "encoding/json", "fmt", "log", "math/rand", "net/http",
        "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
        "github.com/redis/go-redis/v9",
    ]
    code = go_header("fraud-detection", 8111, imports)
    code += go_prom_and_middleware()
    code += '''
var fraudChecks = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "fraud_checks_total", Help: "Fraud checks"},
\t[]string{"risk_level"},
)

func init() {
\tprometheus.MustRegister(fraudChecks)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_redis_setup()
    code += go_health_and_metrics("fraud-detection")
    code += '''
var ctx = context.Background()
var analyticsServiceURL string

func fraudCheckHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tamount := 100.0
\tfmt.Sscanf(r.URL.Query().Get("amount"), "%f", &amount)
\thighRate := false
\tif redisClient != nil {
\t\tkey := "fraud:rate:" + userID
\t\tcount, err := redisClient.Incr(ctx, key).Result()
\t\tif err == nil {
\t\t\tif count == 1 {
\t\t\t\tredisClient.Expire(ctx, key, 60*time.Second)
\t\t\t}
\t\t\thighRate = count > 20
\t\t}
\t}
\trisk := "low"
\tif amount > 5000 || highRate {
\t\trisk = "high"
\t} else if amount > 1000 {
\t\trisk = "medium"
\t}
\tfraudChecks.WithLabelValues(risk).Inc()
\t// Fire-and-forget: track fraud signals in analytics
\tgo func() {
\t\t_, err := httpPost(analyticsServiceURL + "/analytics/signals?user_id=" + url.QueryEscape(userID) + "&event=fraud_check&risk=" + url.QueryEscape(risk))
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: analytics-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "amount": amount, "risk": risk, "approved": risk != "high",
\t})
}
'''
    code += go_main_func(
        [
            'redisURL := getEnv("REDIS_URL", "redis://redis:6379/9")',
            'initRedis(redisURL)',
            'analyticsServiceURL = getEnv("ANALYTICS_SERVICE_URL", "http://analytics-service:8107")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/fraud/check", metricsMiddleware(fraudCheckHandler))',
        ],
    )
    return code


def gen_tax_service():
    imports = [
        "encoding/json", "fmt", "log", "math", "math/rand",
        "net/http", "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("tax-service", 8112, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("tax-service")
    code += '''
func calculateTaxHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tamount := 100.0
\tfmt.Sscanf(r.URL.Query().Get("amount"), "%f", &amount)
\tregion := r.URL.Query().Get("region")
\tif region == "" { region = "US" }
\trates := map[string]float64{"US": 0.08, "EU": 0.20, "UK": 0.20, "CA": 0.13}
\trate := rates[region]
\tif rate == 0 { rate = 0.08 }
\ttax := math.Round(amount*rate*100) / 100
\ttotal := math.Round((amount+tax)*100) / 100
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"amount": amount, "region": region, "rate": rate, "tax": tax, "total": total,
\t})
}
'''
    code += go_main_func(
        [],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/tax/calculate", metricsMiddleware(calculateTaxHandler))',
        ],
    )
    return code


def gen_warehouse_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "os", "strconv", "strings", "sync", "time",
        "github.com/IBM/sarama",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("warehouse-service", 8113, imports)
    code += go_prom_and_middleware()
    code += '''
var warehouseOps = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "warehouse_operations_total", Help: "Warehouse operations"},
\t[]string{"operation"},
)

func init() {
\tprometheus.MustRegister(warehouseOps)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_kafka_producer_setup()
    code += go_health_and_metrics("warehouse-service")
    code += '''
func checkHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\titems := r.URL.Query().Get("items")
\tif items == "" { items = "item-1" }
\twarehouseOps.WithLabelValues("check").Inc()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"items": strings.Split(items, ","), "available": true,
\t\t"location": fmt.Sprintf("aisle-%d", rand.Intn(50)+1),
\t})
}

func dispatchHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\torderID := r.URL.Query().Get("order_id")
\tif orderID == "" { orderID = "unknown" }
\twarehouseOps.WithLabelValues("dispatch").Inc()
\tkafkaSend("regression-lab-notifications", map[string]interface{}{
\t\t"email":   "warehouse@example.com",
\t\t"subject": fmt.Sprintf("Order %s dispatched", orderID),
\t\t"body":    fmt.Sprintf("Order %s has been dispatched from warehouse", orderID),
\t})
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"order_id": orderID, "status": "dispatched",
\t\t"tracking": fmt.Sprintf("TRK-%d", rand.Intn(900000)+100000),
\t})
}
'''
    code += go_main_func(
        [
            'kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")',
            'go initKafkaProducer(kafkaBrokers)',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/warehouse/check", metricsMiddleware(checkHandler))',
            'mux.HandleFunc("/warehouse/dispatch", metricsMiddleware(dispatchHandler))',
        ],
    )
    return code


def gen_media_service():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "net/url", "os", "strconv", "strings", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("media-service", 8114, imports)
    code += go_prom_and_middleware()
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("media-service")
    code += '''
var cacheServiceURL string

func productMediaHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tproductID := strings.TrimPrefix(r.URL.Path, "/media/product/")
\tnumImages := rand.Intn(3) + 2
\timages := make([]string, numImages)
\tfor i := range images {
\t\timages[i] = fmt.Sprintf("/img/%s_%d.jpg", productID, i+1)
\t}
\t// Fire-and-forget: cache media URLs
\tgo func() {
\t\t_, err := httpPost(cacheServiceURL + "/cache/set?key=" + url.QueryEscape("media:"+productID) + "&value=" + url.QueryEscape(productID) + "&ttl=600")
\t\tif err != nil {
\t\t\tlog.Printf("WARNING: cache-service call failed: %v", err)
\t\t}
\t}()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"product_id": productID, "images": images,
\t})
}

func avatarHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tuserID := r.URL.Query().Get("user_id")
\tif userID == "" { userID = "user-1" }
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"user_id": userID, "url": fmt.Sprintf("/avatars/%s.png", userID),
\t})
}

func mediaRouter(w http.ResponseWriter, r *http.Request) {
\tpath := r.URL.Path
\tif strings.HasPrefix(path, "/media/product/") {
\t\tproductMediaHandler(w, r)
\t\treturn
\t}
\tif path == "/media/avatar" {
\t\tavatarHandler(w, r)
\t\treturn
\t}
\thttp.NotFound(w, r)
}
'''
    code += go_main_func(
        [
            'cacheServiceURL = getEnv("CACHE_SERVICE_URL", "http://cache-service:8109")',
        ],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/media/", metricsMiddleware(mediaRouter))',
            'mux.HandleFunc("/media/avatar", metricsMiddleware(avatarHandler))',
        ],
    )
    return code


def gen_external_payment_api():
    imports = [
        "encoding/json", "fmt", "log", "math/rand",
        "net/http", "os", "strconv", "sync", "time",
        "github.com/prometheus/client_golang/prometheus",
        "github.com/prometheus/client_golang/prometheus/promhttp",
    ]
    code = go_header("external-payment-api", 8115, imports)
    code += go_prom_and_middleware()
    code += '''
var charges = prometheus.NewCounterVec(
\tprometheus.CounterOpts{Name: "external_charges_total", Help: "External charges"},
\t[]string{"status"},
)

func init() {
\tprometheus.MustRegister(charges)
}
'''
    code += go_json_helpers()
    code += go_fault_injection()
    code += go_health_and_metrics("external-payment-api")
    code += '''
func chargeHandler(w http.ResponseWriter, r *http.Request) {
\tif applyFaultInjection(r.Context(), w) { return }
\tinvoiceID := r.URL.Query().Get("invoice_id")
\tamount := 100.0
\tfmt.Sscanf(r.URL.Query().Get("amount"), "%f", &amount)
\t// Simulate variable external API latency (50-300ms)
\ttime.Sleep(time.Duration(50+rand.Intn(250)) * time.Millisecond)
\t// 2% random failure rate
\tif rand.Float64() < 0.02 {
\t\tcharges.WithLabelValues("failed").Inc()
\t\thttp.Error(w, "Payment gateway temporarily unavailable", http.StatusBadGateway)
\t\treturn
\t}
\ttxnID := fmt.Sprintf("ext-%d", time.Now().UnixNano()%1000000000000)
\tcharges.WithLabelValues("success").Inc()
\tjsonResponse(w, http.StatusOK, map[string]interface{}{
\t\t"transaction_id": txnID, "invoice_id": invoiceID, "amount": amount, "status": "charged",
\t})
}
'''
    code += go_main_func(
        [],
        [
            'mux.HandleFunc("/admin/config", adminConfigHandler)',
            'mux.HandleFunc("/external/charge", metricsMiddleware(chargeHandler))',
        ],
    )
    return code


# -- Map service names to generators -------------------------------------------

SERVICE_GENERATORS = {
    "auth-service": gen_auth_service,
    "orders-service": gen_orders_service,
    "inventory-service": gen_inventory_service,
    "shipping-service": gen_shipping_service,
    "search-service": gen_search_service,
    "ranking-service": gen_ranking_service,
    "profile-service": gen_profile_service,
    "billing-service": gen_billing_service,
    "payment-adapter": gen_payment_adapter,
    "ingest-service": gen_ingest_service,
    "processing-service": gen_processing_service,
    "recommendation-service": gen_recommendation_service,
    "delivery-service": gen_delivery_service,
    "pricing-service": gen_pricing_service,
    "discount-service": gen_discount_service,
    "user-service": gen_user_service,
    "notification-service": gen_notification_service,
    "cart-service": gen_cart_service,
    "catalog-service": gen_catalog_service,
    "review-service": gen_review_service,
    "loyalty-service": gen_loyalty_service,
    "audit-service": gen_audit_service,
    "session-service": gen_session_service,
    "analytics-service": gen_analytics_service,
    "email-service": gen_email_service,
    "cache-service": gen_cache_service,
    "reporting-service": gen_reporting_service,
    "fraud-detection": gen_fraud_detection,
    "tax-service": gen_tax_service,
    "warehouse-service": gen_warehouse_service,
    "media-service": gen_media_service,
    "external-payment-api": gen_external_payment_api,
}


# -- Generate service files ----------------------------------------------------

def generate_services():
    for svc_name, (port, needs_consumer, needs_producer, needs_redis) in SERVICES.items():
        if svc_name in KEEP_EXISTING:
            continue

        svc_dir = os.path.join(SERVICES_DIR, svc_name)
        os.makedirs(svc_dir, exist_ok=True)

        # Write main.go
        if svc_name in SERVICE_GENERATORS:
            content = SERVICE_GENERATORS[svc_name]()
        else:
            raise ValueError(f"No generator for {svc_name}")

        with open(os.path.join(svc_dir, "main.go"), "w") as f:
            f.write(content)

        # Write go.mod
        needs_kafka = needs_consumer or needs_producer
        with open(os.path.join(svc_dir, "go.mod"), "w") as f:
            f.write(gen_go_mod(svc_name, needs_kafka, needs_redis))

        # Write empty go.sum placeholder
        with open(os.path.join(svc_dir, "go.sum"), "w") as f:
            f.write("")

        # Write Dockerfile
        with open(os.path.join(svc_dir, "Dockerfile"), "w") as f:
            f.write(gen_dockerfile(svc_name, port))

        print(f"  Generated {svc_name} (port {port})")


# -- Generate k8s/03-app.yaml -------------------------------------------------

# Environment variables per service
SERVICE_ENV = {
    "frontend":              {"API_GATEWAY_URL": "http://api-gateway:8081", "SEARCH_SERVICE_URL": "http://search-service:8088",
                              "PROFILE_SERVICE_URL": "http://profile-service:8090", "INGEST_SERVICE_URL": "http://ingest-service:8093"},
    "api-gateway":           {"CHECKOUT_URL": "http://checkout:8082", "AUTH_SERVICE_URL": "http://auth-service:8084",
                              "CART_SERVICE_URL": "http://cart-service:8101", "CATALOG_SERVICE_URL": "http://catalog-service:8102",
                              "ORDERS_SERVICE_URL": "http://orders-service:8085", "PROFILE_SERVICE_URL": "http://profile-service:8090"},
    "checkout":              {"PAYMENTS_API_URL": "http://payments-api:8083", "REDIS_URL": "redis://redis:6379/0",
                              "KAFKA_BROKERS": "kafka:9092", "PAYMENTS_TIMEOUT_SECONDS": "3.0",
                              "PRICING_SERVICE_URL": "http://pricing-service:8097",
                              "BILLING_SERVICE_URL": "http://billing-service:8091",
                              "FRAUD_DETECTION_URL": "http://fraud-detection:8111"},
    "payments-api":          {"DATABASE_URL": "postgresql://postgres:password@payments-db:5432/payments"},
    "auth-service":          {"SESSION_SERVICE_URL": "http://session-service:8106", "USER_SERVICE_URL": "http://user-service:8099",
                              "KAFKA_BROKERS": "kafka:9092"},
    "orders-service":        {"INVENTORY_SERVICE_URL": "http://inventory-service:8086",
                              "NOTIFICATION_SERVICE_URL": "http://notification-service:8100",
                              "SHIPPING_SERVICE_URL": "http://shipping-service:8087",
                              "KAFKA_BROKERS": "kafka:9092"},
    "inventory-service":     {"WAREHOUSE_SERVICE_URL": "http://warehouse-service:8113",
                              "NOTIFICATION_SERVICE_URL": "http://notification-service:8100",
                              "REDIS_URL": "redis://redis:6379/1",
                              "KAFKA_BROKERS": "kafka:9092"},
    "shipping-service":      {"WAREHOUSE_SERVICE_URL": "http://warehouse-service:8113",
                              "NOTIFICATION_SERVICE_URL": "http://notification-service:8100",
                              "EMAIL_SERVICE_URL": "http://email-service:8108",
                              "ANALYTICS_SERVICE_URL": "http://analytics-service:8107",
                              "KAFKA_BROKERS": "kafka:9092"},
    "search-service":        {"RANKING_SERVICE_URL": "http://ranking-service:8089",
                              "CACHE_SERVICE_URL": "http://cache-service:8109",
                              "ANALYTICS_SERVICE_URL": "http://analytics-service:8107",
                              "KAFKA_BROKERS": "kafka:9092"},
    "ranking-service":       {"PROFILE_SERVICE_URL": "http://profile-service:8090",
                              "ANALYTICS_SERVICE_URL": "http://analytics-service:8107",
                              "RECOMMENDATION_SERVICE_URL": "http://recommendation-service:8095",
                              "REDIS_URL": "redis://redis:6379/2"},
    "profile-service":       {"USER_SERVICE_URL": "http://user-service:8099", "MEDIA_SERVICE_URL": "http://media-service:8114",
                              "LOYALTY_SERVICE_URL": "http://loyalty-service:8104",
                              "CACHE_SERVICE_URL": "http://cache-service:8109"},
    "billing-service":       {"PAYMENT_ADAPTER_URL": "http://payment-adapter:8092", "TAX_SERVICE_URL": "http://tax-service:8112",
                              "FRAUD_DETECTION_URL": "http://fraud-detection:8111",
                              "NOTIFICATION_SERVICE_URL": "http://notification-service:8100",
                              "KAFKA_BROKERS": "kafka:9092"},
    "payment-adapter":       {"EXTERNAL_PAYMENT_URL": "http://external-payment-api:8115",
                              "NOTIFICATION_SERVICE_URL": "http://notification-service:8100"},
    "ingest-service":        {"KAFKA_BROKERS": "kafka:9092"},
    "processing-service":    {"KAFKA_BROKERS": "kafka:9092"},
    "recommendation-service":{"ANALYTICS_SERVICE_URL": "http://analytics-service:8107",
                              "DELIVERY_SERVICE_URL": "http://delivery-service:8096",
                              "USER_SERVICE_URL": "http://user-service:8099",
                              "CACHE_SERVICE_URL": "http://cache-service:8109",
                              "REDIS_URL": "redis://redis:6379/3", "KAFKA_BROKERS": "kafka:9092"},
    "delivery-service":      {"NOTIFICATION_SERVICE_URL": "http://notification-service:8100",
                              "WAREHOUSE_SERVICE_URL": "http://warehouse-service:8113",
                              "EMAIL_SERVICE_URL": "http://email-service:8108"},
    "pricing-service":       {"DISCOUNT_SERVICE_URL": "http://discount-service:8098",
                              "TAX_SERVICE_URL": "http://tax-service:8112",
                              "CACHE_SERVICE_URL": "http://cache-service:8109",
                              "REDIS_URL": "redis://redis:6379/4"},
    "discount-service":      {"LOYALTY_SERVICE_URL": "http://loyalty-service:8104",
                              "CACHE_SERVICE_URL": "http://cache-service:8109"},
    "user-service":          {"CACHE_SERVICE_URL": "http://cache-service:8109"},
    "notification-service":  {"EMAIL_SERVICE_URL": "http://email-service:8108", "KAFKA_BROKERS": "kafka:9092"},
    "cart-service":          {"CATALOG_SERVICE_URL": "http://catalog-service:8102",
                              "PRICING_SERVICE_URL": "http://pricing-service:8097",
                              "SESSION_SERVICE_URL": "http://session-service:8106",
                              "REDIS_URL": "redis://redis:6379/5"},
    "catalog-service":       {"REVIEW_SERVICE_URL": "http://review-service:8103", "MEDIA_SERVICE_URL": "http://media-service:8114",
                              "CACHE_SERVICE_URL": "http://cache-service:8109",
                              "PRICING_SERVICE_URL": "http://pricing-service:8097",
                              "INVENTORY_SERVICE_URL": "http://inventory-service:8086"},
    "review-service":        {"RECOMMENDATION_SERVICE_URL": "http://recommendation-service:8095",
                              "USER_SERVICE_URL": "http://user-service:8099",
                              "MEDIA_SERVICE_URL": "http://media-service:8114",
                              "KAFKA_BROKERS": "kafka:9092"},
    "loyalty-service":       {"USER_SERVICE_URL": "http://user-service:8099",
                              "CACHE_SERVICE_URL": "http://cache-service:8109",
                              "REDIS_URL": "redis://redis:6379/6"},
    "audit-service":         {"KAFKA_BROKERS": "kafka:9092"},
    "session-service":       {"REDIS_URL": "redis://redis:6379/7"},
    "analytics-service":     {"REPORTING_SERVICE_URL": "http://reporting-service:8110",
                              "CACHE_SERVICE_URL": "http://cache-service:8109",
                              "KAFKA_BROKERS": "kafka:9092"},
    "email-service":         {},
    "cache-service":         {"REDIS_URL": "redis://redis:6379/8"},
    "reporting-service":     {"CACHE_SERVICE_URL": "http://cache-service:8109"},
    "fraud-detection":       {"REDIS_URL": "redis://redis:6379/9",
                              "ANALYTICS_SERVICE_URL": "http://analytics-service:8107"},
    "tax-service":           {},
    "warehouse-service":     {"KAFKA_BROKERS": "kafka:9092"},
    "media-service":         {"CACHE_SERVICE_URL": "http://cache-service:8109"},
    "external-payment-api":  {},
}

# Services that need to wait for Kafka
NEEDS_KAFKA = {"checkout", "auth-service", "orders-service", "inventory-service", "shipping-service",
               "search-service", "billing-service", "ingest-service", "processing-service",
               "recommendation-service", "notification-service", "review-service", "audit-service",
               "analytics-service", "warehouse-service"}

# Services that need to wait for DB
NEEDS_DB = {"payments-api"}

# Services that run 3 replicas with HPA (high-traffic / critical-path)
SCALED_SERVICES = {
    "frontend", "api-gateway", "checkout", "payments-api",
    "search-service", "orders-service", "catalog-service",
    "user-service", "auth-service", "cart-service",
    "recommendation-service", "ranking-service",
}


def gen_k8s_app_yaml():
    """Generate the complete k8s/03-app.yaml."""
    lines = []
    all_svc_names = sorted(SERVICES.keys())

    for svc_name in all_svc_names:
        port = SERVICES[svc_name][0]
        env_vars = SERVICE_ENV.get(svc_name, {})
        svc_type = "NodePort" if svc_name == "frontend" else "ClusterIP"
        failure_threshold = 10 if svc_name in (NEEDS_DB | NEEDS_KAFKA) else 5

        lines.append("---")
        lines.append(f"# -- {svc_name} {'--' * (30 - len(svc_name))}")
        lines.append("apiVersion: apps/v1")
        lines.append("kind: Deployment")
        lines.append("metadata:")
        lines.append(f"  name: {svc_name}")
        lines.append("  namespace: scenario-01")
        lines.append("  labels:")
        lines.append("    app.kubernetes.io/name: scenario-01")
        lines.append(f"    service: {svc_name}")
        replicas = 1  # start at 1; HPA will scale up to 10 under load
        lines.append("spec:")
        lines.append(f"  replicas: {replicas}")
        lines.append("  selector:")
        lines.append("    matchLabels:")
        lines.append(f"      app: {svc_name}")
        lines.append("  template:")
        lines.append("    metadata:")
        lines.append("      labels:")
        lines.append(f"        app: {svc_name}")
        lines.append(f"        service: {svc_name}")
        lines.append("    spec:")

        # Init containers
        if svc_name in NEEDS_DB:
            lines.append("      initContainers:")
            lines.append("        - name: wait-for-db")
            lines.append("          image: postgres:16-alpine")
            lines.append("          command:")
            lines.append("            - sh")
            lines.append("            - -c")
            lines.append('            - until pg_isready -h payments-db -p 5432 -U postgres; do echo "waiting for payments-db..."; sleep 2; done')
        elif svc_name in NEEDS_KAFKA:
            lines.append("      initContainers:")
            lines.append("        - name: wait-for-kafka")
            lines.append("          image: busybox:1.36")
            lines.append("          command:")
            lines.append("            - sh")
            lines.append("            - -c")
            lines.append('            - until nc -z kafka 9092; do echo "waiting for kafka..."; sleep 2; done')

        lines.append("      containers:")
        lines.append(f"        - name: {svc_name}")
        lines.append(f"          image: docker.io/causely-oss/{svc_name}:v1")
        lines.append("          imagePullPolicy: IfNotPresent")
        lines.append("          ports:")
        lines.append(f"            - containerPort: {port}")

        # Resource requests (required for HPA) on scaled services.
        # cart-service OOMs at 256Mi under load; keep a higher ceiling there.
        if svc_name in SCALED_SERVICES:
            mem_request, mem_limit = ("256Mi", "512Mi") if svc_name == "cart-service" else ("128Mi", "256Mi")
            lines.append("          resources:")
            lines.append("            requests:")
            lines.append("              cpu: 100m")
            lines.append(f"              memory: {mem_request}")
            lines.append("            limits:")
            lines.append("              cpu: 500m")
            lines.append(f"              memory: {mem_limit}")

        lines.append("          env:")
        lines.append("            - name: OTEL_EXPORTER_OTLP_ENDPOINT")
        lines.append('              value: "http://otel-collector:4318"')
        lines.append("            - name: OTEL_EXPORTER_OTLP_INSECURE")
        lines.append('              value: "true"')
        lines.append("            - name: OTEL_TRACES_EXPORTER")
        lines.append('              value: "otlp"')
        lines.append("            - name: OTEL_METRICS_EXPORTER")
        lines.append('              value: "otlp"')
        lines.append("            - name: OTEL_PROPAGATORS")
        lines.append('              value: "tracecontext,baggage"')
        lines.append("            - name: OTEL_SERVICE_NAME")
        lines.append(f'              value: "{svc_name}"')
        if env_vars:
            for k, v in env_vars.items():
                lines.append(f"            - name: {k}")
                lines.append(f'              value: "{v}"')

        lines.append("          readinessProbe:")
        lines.append("            httpGet:")
        lines.append("              path: /health")
        lines.append(f"              port: {port}")
        lines.append("            initialDelaySeconds: 10")
        lines.append("            periodSeconds: 10")
        lines.append(f"            failureThreshold: {failure_threshold}")

        lines.append("")
        lines.append("---")
        lines.append("apiVersion: v1")
        lines.append("kind: Service")
        lines.append("metadata:")
        lines.append(f"  name: {svc_name}")
        lines.append("  namespace: scenario-01")
        lines.append("  labels:")
        lines.append("    app.kubernetes.io/name: scenario-01")
        lines.append(f"    service: {svc_name}")
        lines.append("spec:")
        lines.append(f"  type: {svc_type}")
        lines.append("  selector:")
        lines.append(f"    app: {svc_name}")
        lines.append("  ports:")
        lines.append(f"    - port: {port}")
        lines.append(f"      targetPort: {port}")
        if svc_name == "frontend":
            lines.append("      nodePort: 30080")

        lines.append("")

    return "\n".join(lines)


def gen_k8s_hpa_yaml():
    """Generate k8s/05-hpa.yaml for scaled services."""
    lines = []
    for svc_name in sorted(SCALED_SERVICES):
        port = SERVICES[svc_name][0]
        lines.append("---")
        lines.append(f"# -- HPA: {svc_name} {'--' * (27 - len(svc_name))}")
        lines.append("apiVersion: autoscaling/v2")
        lines.append("kind: HorizontalPodAutoscaler")
        lines.append("metadata:")
        lines.append(f"  name: {svc_name}")
        lines.append("  namespace: scenario-01")
        lines.append("spec:")
        lines.append("  scaleTargetRef:")
        lines.append("    apiVersion: apps/v1")
        lines.append("    kind: Deployment")
        lines.append(f"    name: {svc_name}")
        lines.append("  minReplicas: 1")
        lines.append("  maxReplicas: 10")
        lines.append("  metrics:")
        lines.append("    - type: Resource")
        lines.append("      resource:")
        lines.append("        name: cpu")
        lines.append("        target:")
        lines.append("          type: Utilization")
        lines.append("          averageUtilization: 70")
        lines.append("")
    return "\n".join(lines)


# -- Generate docker-compose.yml -----------------------------------------------

def gen_docker_compose():
    lines = [
        'name: scenario-01',
        '',
        'services:',
        '',
        '  # -- Application services ---------------------------------------------------',
    ]

    all_svc_names = sorted(SERVICES.keys())
    for svc_name in all_svc_names:
        port = SERVICES[svc_name][0]
        env_vars = SERVICE_ENV.get(svc_name, {})

        lines.append(f'')
        lines.append(f'  {svc_name}:')
        lines.append(f'    build: ./services/{svc_name}')
        lines.append(f'    ports:')
        lines.append(f'      - "{port}:{port}"')

        if env_vars:
            lines.append(f'    environment:')
            for k, v in env_vars.items():
                lines.append(f'      {k}: {v}')

        # Dependencies
        deps = []
        if svc_name in NEEDS_DB:
            deps.append(('payments-db', 'service_healthy'))
        if svc_name in NEEDS_KAFKA:
            deps.append(('kafka', 'service_healthy'))
        # Redis dependencies
        if SERVICES[svc_name][3]:  # needs_redis
            deps.append(('redis', 'service_healthy'))

        if deps:
            lines.append(f'    depends_on:')
            for dep, condition in deps:
                lines.append(f'      {dep}:')
                lines.append(f'        condition: {condition}')

        lines.append(f'    healthcheck:')
        lines.append(f'      test: ["CMD", "wget", "--spider", "-q", "http://localhost:{port}/health"]')
        lines.append(f'      interval: 10s')
        lines.append(f'      timeout: 5s')
        lines.append(f'      retries: 5')

    # Data stores
    lines.append('')
    lines.append('  # -- Data stores ---------------------------------------------------------')
    lines.append('')
    lines.append(textwrap.dedent('''\
      payments-db:
        image: postgres:16-alpine
        environment:
          POSTGRES_DB: payments
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: password
        ports:
          - "5432:5432"
        volumes:
          - ./init.sql:/docker-entrypoint-initdb.d/init.sql
          - payments-db-data:/var/lib/postgresql/data
        healthcheck:
          test: ["CMD-SHELL", "pg_isready -U postgres -d payments"]
          interval: 5s
          timeout: 3s
          retries: 10

      redis:
        image: redis:7-alpine
        ports:
          - "6379:6379"
        command: redis-server --maxmemory 512mb --maxmemory-policy allkeys-lru
        healthcheck:
          test: ["CMD", "redis-cli", "ping"]
          interval: 5s
          timeout: 3s
          retries: 5

      kafka:
        image: confluentinc/cp-kafka:7.6.0
        ports:
          - "9092:9092"
        environment:
          KAFKA_NODE_ID: 1
          KAFKA_PROCESS_ROLES: broker,controller
          KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
          KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
          KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
          KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
          CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
        healthcheck:
          test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
          interval: 15s
          timeout: 10s
          retries: 10'''))

    # Observability
    lines.append('')
    lines.append('  # -- Observability --------------------------------------------------------')
    lines.append('')

    prom_deps = "    depends_on:\n" + "\n".join(f"      - {s}" for s in sorted(SERVICES.keys())[:10])

    lines.append(textwrap.dedent(f'''\
      prometheus:
        image: prom/prometheus:v2.51.0
        ports:
          - "9090:9090"
        volumes:
          - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
        command:
          - "--config.file=/etc/prometheus/prometheus.yml"
          - "--storage.tsdb.retention.time=1h"
    {prom_deps}

      grafana:
        image: grafana/grafana:10.4.0
        ports:
          - "3000:3000"
        environment:
          GF_SECURITY_ADMIN_PASSWORD: admin
          GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /var/lib/grafana/dashboards/scenario-01.json
        volumes:
          - ./grafana-datasource.yml:/etc/grafana/provisioning/datasources/prometheus.yml:ro
          - ./grafana-dashboard.json:/var/lib/grafana/dashboards/scenario-01.json:ro
          - grafana-data:/var/lib/grafana
        depends_on:
          - prometheus

      postgres-exporter:
        image: prometheuscommunity/postgres-exporter:v0.15.0
        environment:
          DATA_SOURCE_NAME: "postgresql://postgres:password@payments-db:5432/payments?sslmode=disable"
        ports:
          - "9187:9187"
        depends_on:
          payments-db:
            condition: service_healthy

      redis-exporter:
        image: oliver006/redis_exporter:v1.58.0
        environment:
          REDIS_ADDR: redis://redis:6379
        ports:
          - "9121:9121"
        depends_on:
          - redis'''))

    lines.append('')
    lines.append('volumes:')
    lines.append('  payments-db-data:')
    lines.append('  grafana-data:')

    return "\n".join(lines)


# -- Generate prometheus.yml ---------------------------------------------------

def gen_prometheus_yml():
    lines = [
        'global:',
        '  scrape_interval: 15s',
        '  evaluation_interval: 15s',
        '',
        'scrape_configs:',
    ]
    for svc_name in sorted(SERVICES.keys()):
        port = SERVICES[svc_name][0]
        lines.append(f'  - job_name: {svc_name}')
        lines.append(f'    static_configs:')
        lines.append(f"      - targets: ['{svc_name}:{port}']")
        lines.append('')

    lines.append('  - job_name: postgres-exporter')
    lines.append('    static_configs:')
    lines.append("      - targets: ['postgres-exporter:9187']")
    lines.append('')
    lines.append('  - job_name: redis-exporter')
    lines.append('    static_configs:')
    lines.append("      - targets: ['redis-exporter:9121']")

    return "\n".join(lines)


# -- Generate build-images.sh -------------------------------------------------

def gen_build_script():
    svc_list = " ".join(sorted(SERVICES.keys()))
    return textwrap.dedent(f'''\
    #!/usr/bin/env bash
    # build-images.sh
    #
    # Builds all custom service images directly into minikube's Docker daemon.
    # Must be run before applying the Kubernetes manifests.
    #
    # Usage:
    #   bash k8s/build-images.sh

    set -euo pipefail

    SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
    SERVICES_DIR="$SCRIPT_DIR/../environment/services"

    echo "=== Pointing Docker CLI at minikube's daemon ==="
    eval "$(minikube docker-env)"

    SERVICES=({svc_list})

    for svc in "${{SERVICES[@]}}"; do
      echo ""
      echo "=== Building scenario01/$svc:latest ==="
      docker build -t "scenario01/$svc:latest" "$SERVICES_DIR/$svc"
    done

    echo ""
    echo "=== All ${{#SERVICES[@]}} images built ==="
    docker images | grep scenario01
    ''')


# -- Main ---------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Generating {len(SERVICES)} services (Go)...")
    print()

    print("1. Generating service main.go, go.mod, and Dockerfile files:")
    generate_services()
    print()

    print("2. Generating k8s/03-app.yaml...")
    with open(os.path.join(BASE, "k8s", "03-app.yaml"), "w") as f:
        f.write(gen_k8s_app_yaml())
    print("   Done.")

    print("2b. Generating k8s/05-hpa.yaml...")
    with open(os.path.join(BASE, "k8s", "05-hpa.yaml"), "w") as f:
        f.write(gen_k8s_hpa_yaml())
    print(f"    Done ({len(SCALED_SERVICES)} HPAs).")

    print("3. Generating environment/docker-compose.yml...")
    with open(os.path.join(BASE, "environment", "docker-compose.yml"), "w") as f:
        f.write(gen_docker_compose())
    print("   Done.")

    print("4. Generating environment/prometheus.yml...")
    with open(os.path.join(BASE, "environment", "prometheus.yml"), "w") as f:
        f.write(gen_prometheus_yml())
    print("   Done.")

    # NOTE: k8s/build-images.sh is maintained manually (versioned tags, buildx, --push).
    # Do not overwrite it here.
    print("5. Skipping k8s/build-images.sh (manually maintained).")

    print()
    print(f"Total: {len(SERVICES)} services generated (32 Go services + 4 KEEP_EXISTING).")
    print()
    print("Service communication flows:")
    print("  Auth:      frontend -> api-gateway -> auth-service -> session-service, user-service")
    print("  Orders:    checkout -> Kafka(regression-lab-orders) -> orders-service -> inventory -> Kafka(regression-lab-shipping-events) -> shipping -> warehouse")
    print("  Search:    frontend -> search-service -> ranking-service -> profile-service -> user-service")
    print("  Deep:      frontend -> api-gateway -> catalog -> review -> recommendation -> analytics -> reporting")
    print("  Billing:   checkout -> billing -> payment-adapter -> external-payment-api")
    print("  Streaming: ingest -> Kafka(regression-lab-ingest-data) -> processing -> Kafka(regression-lab-recommendations) -> recommendation -> delivery")
    print("  Orders UI: frontend -> api-gateway -> orders-service -> inventory -> warehouse")
    print("  Checkout:  checkout -> pricing -> discount -> loyalty")
    print()
    print("Kafka topics: regression-lab-orders, regression-lab-inventory-updates, regression-lab-shipping-events, regression-lab-notifications, regression-lab-audit-events, regression-lab-analytics-events, regression-lab-ingest-data, regression-lab-recommendations")
