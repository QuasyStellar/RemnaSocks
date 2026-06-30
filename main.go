package main

import (
	"xray-orchestrator/internal/orchestrator"
)

func main() {
	orchestrator.InitLogger()
	server := orchestrator.NewServer()

	go server.StartWebhookUnixServer()
	server.StartSocks5Server()
}
