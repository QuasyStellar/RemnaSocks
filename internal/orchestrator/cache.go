package orchestrator

import (
	"sync"
	"time"
)

type CacheEntry struct {
	Email     string
	Country   string
	Timestamp time.Time
}

type AuthCacheEntry struct {
	IsAllowed   bool
	MultiProxy  map[string]ProxyList
	SingleProxy ProxyList
	ExpiresAt   time.Time
}

type WebhookCache struct {
	mu    sync.RWMutex
	items map[string]CacheEntry
	ttl   time.Duration
}

func NewWebhookCache(ttl time.Duration) *WebhookCache {
	c := &WebhookCache{
		items: make(map[string]CacheEntry),
		ttl:   ttl,
	}
	go c.cleanupWorker()
	return c
}

func (c *WebhookCache) Set(key string, email, country string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = CacheEntry{
		Email:     email,
		Country:   country,
		Timestamp: time.Now(),
	}
}

func (c *WebhookCache) Get(key string) (CacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	item, ok := c.items[key]
	return item, ok
}

func (c *WebhookCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *WebhookCache) cleanupWorker() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.items {
			if now.Sub(v.Timestamp) > c.ttl {
				delete(c.items, k)
			}
		}
		c.mu.Unlock()
	}
}

type AuthCache struct {
	mu    sync.RWMutex
	items map[string]AuthCacheEntry
}

func NewAuthCache() *AuthCache {
	return &AuthCache{
		items: make(map[string]AuthCacheEntry),
	}
}

func (c *AuthCache) Get(key string) (AuthCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.items[key]
	return entry, ok
}

func (c *AuthCache) Set(key string, entry AuthCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = entry
}

func (c *AuthCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}
