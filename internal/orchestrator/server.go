package orchestrator

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type ConnectionSignal struct {
	ch   chan struct{}
	once sync.Once
}

func (s *ConnectionSignal) Signal() {
	s.once.Do(func() {
		close(s.ch)
	})
}

type PendingRegistry struct {
	mu   sync.Mutex
	maps map[string]*ConnectionSignal
}

func NewPendingRegistry() *PendingRegistry {
	return &PendingRegistry{
		maps: make(map[string]*ConnectionSignal),
	}
}

func (r *PendingRegistry) GetOrStore(key string, sig *ConnectionSignal) (*ConnectionSignal, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if actual, ok := r.maps[key]; ok {
		return actual, true
	}
	r.maps[key] = sig
	return sig, false
}

func (r *PendingRegistry) DeleteIfMatch(key string, sig *ConnectionSignal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if actual, ok := r.maps[key]; ok && actual == sig {
		delete(r.maps, key)
	}
}

func (r *PendingRegistry) Signal(key string) {
	r.mu.Lock()
	sig, ok := r.maps[key]
	r.mu.Unlock()
	if ok {
		sig.Signal()
	}
}

type Server struct {
	webhookCache *WebhookCache
	authCache    *AuthCache
	pool         *ProxyPool
	pendingConns *PendingRegistry
	httpClient   *http.Client

	panelURL          string
	panelToken        string
	panelCookie       string
	allowedSquad      string
	socks5ListenAddr  string
	webhookListenAddr string
	webhookTimeout    time.Duration
	authTTL           time.Duration
	unauthTTL         time.Duration
	idleTimeout       time.Duration
}

func NewServer() *Server {
	webhookTTLSec := getEnvIntOrDefault("WEBHOOK_CACHE_TTL_SEC", 10)
	authTTLSec := getEnvIntOrDefault("AUTH_CACHE_TTL_SEC", 60)
	unauthTTLSec := getEnvIntOrDefault("AUTH_BLOCKED_CACHE_TTL_SEC", 30)
	webhookTimeoutMs := getEnvIntOrDefault("WEBHOOK_TIMEOUT_MS", 300)
	idleTimeoutSec := getEnvIntOrDefault("CONNECTION_IDLE_TIMEOUT_SEC", 300)

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	return &Server{
		webhookCache:      NewWebhookCache(time.Duration(webhookTTLSec) * time.Second),
		authCache:         NewAuthCache(),
		pool:              NewProxyPool(),
		pendingConns:      NewPendingRegistry(),
		httpClient:        httpClient,
		panelURL:          os.Getenv("PANEL_URL"),
		panelToken:        os.Getenv("PANEL_TOKEN"),
		panelCookie:       os.Getenv("PANEL_COOKIE"),
		allowedSquad:      os.Getenv("ALLOWED_SQUAD_NAME"),
		socks5ListenAddr:  getEnvOrDefault("SOCKS5_LISTEN_ADDR", "127.0.0.1:1080"),
		webhookListenAddr: getEnvOrDefault("WEBHOOK_LISTEN_ADDR", "@orchestrator-webhook"),
		webhookTimeout:    time.Duration(webhookTimeoutMs) * time.Millisecond,
		authTTL:           time.Duration(authTTLSec) * time.Second,
		unauthTTL:         time.Duration(unauthTTLSec) * time.Second,
		idleTimeout:       time.Duration(idleTimeoutSec) * time.Second,
	}
}

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvIntOrDefault(key string, fallback int) int {
	if valStr, exists := os.LookupEnv(key); exists {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return fallback
}

func normalizeTargetKey(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if ip := net.ParseIP(host); ip != nil {
		return fmt.Sprintf("%s:%s", ip.String(), port)
	}
	return fmt.Sprintf("%s:%s", host, port)
}

type FailureTracker struct {
	mu       sync.Mutex
	failures map[string]time.Time
}

var GlobalFailures = &FailureTracker{
	failures: make(map[string]time.Time),
}

func (t *FailureTracker) MarkFailed(host string, port uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := fmt.Sprintf("%s:%d", host, port)
	t.failures[key] = time.Now()
}

func (t *FailureTracker) IsFailed(host string, port uint16) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := fmt.Sprintf("%s:%d", host, port)
	lastFail, ok := t.failures[key]
	if !ok {
		return false
	}
	if time.Since(lastFail) > 10*time.Second {
		delete(t.failures, key)
		return false
	}
	return true
}
