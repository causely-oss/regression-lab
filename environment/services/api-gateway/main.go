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
func proxyRequest(w http.ResponseWriter, method, url string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		log.Printf("Gateway error building request: %v", err)
		writeError(w, http.StatusBadGateway, "Bad gateway")
		return
	}

	resp, err := http.DefaultClient.Do(req)
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
	proxyRequest(w, http.MethodPost, url, 10*time.Second)
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
	proxyRequest(w, http.MethodPost, url, 10*time.Second)
}

// POST /auth/validate?token=tok-123
func authValidateHandler(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		token = "tok-123"
	}
	url := fmt.Sprintf("%s/auth/validate?token=%s", authServiceURL, token)
	proxyRequest(w, http.MethodPost, url, 5*time.Second)
}

// GET /cart/{user_id}
func cartGetHandler(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimPrefix(r.URL.Path, "/cart/")
	if userID == "" || strings.Contains(userID, "/") {
		writeError(w, http.StatusBadRequest, "missing user_id")
		return
	}
	url := fmt.Sprintf("%s/cart/%s", cartServiceURL, userID)
	proxyRequest(w, http.MethodGet, url, 5*time.Second)
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
	proxyRequest(w, http.MethodPost, url, 5*time.Second)
}

// GET /catalog/products?page=1&category=
func catalogProductsHandler(w http.ResponseWriter, r *http.Request) {
	page := r.URL.Query().Get("page")
	if page == "" {
		page = "1"
	}
	category := r.URL.Query().Get("category")
	url := fmt.Sprintf("%s/catalog/products?page=%s&category=%s", catalogURL, page, category)
	proxyRequest(w, http.MethodGet, url, 10*time.Second)
}

// GET /catalog/product/{product_id}
func catalogProductHandler(w http.ResponseWriter, r *http.Request) {
	productID := strings.TrimPrefix(r.URL.Path, "/catalog/product/")
	if productID == "" || strings.Contains(productID, "/") {
		writeError(w, http.StatusBadRequest, "missing product_id")
		return
	}
	url := fmt.Sprintf("%s/catalog/product/%s", catalogURL, productID)
	proxyRequest(w, http.MethodGet, url, 15*time.Second)
}

// GET /orders?user_id=user-1
func ordersListHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "user-1"
	}
	url := fmt.Sprintf("%s/orders?user_id=%s", ordersURL, userID)
	proxyRequest(w, http.MethodGet, url, 10*time.Second)
}

// GET /orders/{order_id}
func ordersGetHandler(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimPrefix(r.URL.Path, "/orders/")
	if orderID == "" || strings.Contains(orderID, "/") {
		writeError(w, http.StatusBadRequest, "missing order_id")
		return
	}
	url := fmt.Sprintf("%s/orders/%s", ordersURL, orderID)
	proxyRequest(w, http.MethodGet, url, 10*time.Second)
}

// GET /profile/{user_id}
func profileHandler(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimPrefix(r.URL.Path, "/profile/")
	if userID == "" || strings.Contains(userID, "/") {
		writeError(w, http.StatusBadRequest, "missing user_id")
		return
	}
	url := fmt.Sprintf("%s/profile/%s", profileURL, userID)
	proxyRequest(w, http.MethodGet, url, 5*time.Second)
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

	return metricsMiddleware(mux)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
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
