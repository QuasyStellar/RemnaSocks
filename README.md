# RemnaSocks Orchestrator

**RemnaSocks Orchestrator** is a high-performance, Go-based service built for VPN providers using [Remnawave](https://github.com/remnawave) and the Xray core.

---

## Why do you need this?

Normally, a VPN server assigns its own datacenter IP address to the user. However, if you want to provide your clients with **residential IP addresses**, you need to route their traffic through third-party SOCKS5/HTTP residential proxies.

**The Problem:** How do you dynamically figure out which proxy to assign to which user when Xray (VLESS/Shadowsocks) traffic is anonymized and multiplexed?  
**The Solution:** RemnaSocks Orchestrator bridges this gap. It associates incoming Xray connections with real user profiles from the Remnawave Panel and dynamically routes each user's traffic to their personal residential proxy.

---

## Key Features

- **Multi-Protocol Upstream Support**: SOCKS5 (with/without auth) and HTTP CONNECT tunneling (with/without Basic Auth)
- **RFC-Compliant SOCKS5 Negotiation**: Proper method selection with standard rejection codes (`0xFF`)
- **Pre-warmed Connection Pool**: Pre-established upstream connections for near 1 RTT latency
- **TCP Write Pipelining**: Combines SOCKS5 greeting + auth into a single write to reduce RTT
- **Smart Pool Scaling & Zero Descriptor Leak**: Auto-scales based on activity and cleans idle resources
- **Thread-Safe Webhook Matching**: Mutex-protected registry for concurrent routing coordination
- **Flexible Unix Sockets**: Supports both abstract (`@socket`) and filesystem sockets
- **Zero Hardcoded Configuration**: User-defined JSON in Remnawave panel
- **Country-Based Routing**: Routes via `X-Country` header (US / NL / UK etc.)
- **Self-Healing Cache System**: Fallback to cached credentials during API instability
- **IPv6 Support**: Full upstream IPv6 compatibility
- **Strict Privacy (Anti-Leak)**: Blocks traffic if proxy resolution fails (no datacenter IP leaks)
- **Extreme Performance**: ~5MB RAM footprint, O(1) config access, high concurrency support

---

## How It Works (Architecture)

1. Xray accepts encrypted client connection (VLESS/Shadowsocks)
2. Xray sends webhook to Unix socket (`@orchestrator-webhook`) with user ID + destination
3. Orchestrator queries Remnawave Panel API (`/api/users/by-id/{id}`)
4. Xray forwards TCP stream to local SOCKS5 endpoint (`127.0.0.1:1080`)
5. Orchestrator matches webhook → selects proxy → pipes traffic via upstream residential proxy

---

## User Configuration in Remnawave Panel

To assign a proxy, place JSON in the **Description** field.

### Option 1: Country-specific proxy

```json
{
  "US": {
    "type": "http",
    "host": "us-proxy.example.com",
    "port": 8080,
    "user": "proxy_user",
    "pass": "proxy_pass"
  },
  "NL": {
    "type": "socks5",
    "host": "nl-proxy.example.com",
    "port": 3072,
    "user": "proxy_user",
    "pass": "proxy_pass"
  }
}
````

---

### Option 2: Global proxy

```json
{
  "type": "http",
  "host": "global-proxy.example.com",
  "port": 8080
}
```

---

### Backup proxies

You can define multiple proxies per country as an ordered array. RemnaSocks automatically switches to the next proxy if the previous one fails.

```json
{
  "USA": [
    {
      "type": "socks5",
      "host": "PRIMARY_PROXY_HOST",
      "port": 0000,
      "user": "PRIMARY_USER",
      "pass": "PRIMARY_PASS"
    },
    {
      "type": "socks5",
      "host": "BACKUP_PROXY_HOST",
      "port": 0000,
      "user": "BACKUP_USER",
      "pass": "BACKUP_PASS"
    }
  ]
}
```

---

### Global Country Fallback Proxies

If a user does not define a proxy for a specific country:

1. Create a virtual user with a 3-letter country code (e.g. `USA`, `NLD`, `GBR`)
2. Store fallback proxy JSON in its Description field
3. Set `X-Country` header in Xray routing rules

**Routing logic:**

* Numeric user ID → `/api/users/by-id/{id}`
* Country code → `/api/users/by-username/{code}`

---

## Quick Start

### 1. Setup environment

```bash
cp .env.example .env
```

### 2. Configure `.env`

```env
PANEL_URL=https://your-panel-url.com
PANEL_TOKEN=your_api_jwt_token_here

SOCKS5_LISTEN_ADDR=127.0.0.1:1080
WEBHOOK_LISTEN_ADDR=@orchestrator-webhook

LOG_LEVEL=INFO
WEBHOOK_TIMEOUT_MS=300

WEBHOOK_CACHE_TTL_SEC=10
AUTH_CACHE_TTL_SEC=60
AUTH_BLOCKED_CACHE_TTL_SEC=30
```

---

### 3. Run with Docker

```bash
docker compose up -d --build
```

---

## Codebase Architecture

* **main.go** – entry point, service bootstrap
* **internal/orchestrator/server.go** – core server logic & config
* **internal/orchestrator/logger.go** – structured logging system
* **internal/orchestrator/proxy.go** – proxy data models
* **internal/orchestrator/cache.go** – thread-safe caching layer
* **internal/orchestrator/panel.go** – Remnawave API integration
* **internal/orchestrator/pool.go** – pre-warmed connection pool
* **internal/orchestrator/socks.go** – SOCKS5 proxy engine
* **internal/orchestrator/webhook.go** – Unix socket webhook handler (VLESS mapping)
