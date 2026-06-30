package orchestrator

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"
)

type WebhookPayload struct {
	Email       string `json:"email"`
	Destination string `json:"destination"`
}

func (s *Server) webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4096)

	var payload WebhookPayload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if payload.Email != "" && payload.Destination != "" {
		dest := payload.Destination
		if len(dest) > 4 && dest[:4] == "tcp:" {
			dest = dest[4:]
		}

		country := r.Header.Get("X-Country")
		normKey := normalizeTargetKey(dest)

		s.webhookCache.Set(normKey, payload.Email, country)
		s.pendingConns.Signal(normKey)

		logDebug("Webhook: %s -> User: %s, Country: %s", normKey, payload.Email, country)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) StartWebhookUnixServer() {
	addr := s.webhookListenAddr
	if !strings.HasPrefix(addr, "@") {
		_ = os.Remove(addr)
	}
	listener, err := net.Listen("unix", addr)
	if err != nil {
		logError("Webhook Bind Failed: %v", err)
		return
	}
	defer listener.Close()
	logInfo("Webhook Listening on: %s", addr)

	server := &http.Server{
		Handler: http.HandlerFunc(s.webhookHandler),
	}
	server.Serve(listener)
}
