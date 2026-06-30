package orchestrator

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

func (s *Server) StartSocks5Server() {
	addr := s.socks5ListenAddr
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logError("SOCKS5 Bind Failed: %v", err)
		os.Exit(1)
	}
	defer listener.Close()
	logInfo("SOCKS5 Listening on: %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go s.handleSocksClient(conn)
	}
}

func (s *Server) handleSocksClient(client net.Conn) {
	defer client.Close()

	if err := s.readHandshake(client); err != nil {
		return
	}

	destAddr, destPort, err := s.readDestination(client)
	if err != nil {
		return
	}

	targetKey := normalizeTargetKey(fmt.Sprintf("%s:%d", destAddr, destPort))
	userEmail, userCountry := s.waitWebhook(targetKey)
	if userEmail == "" {
		logWarn("SOCKS5 BLOCKED: Unauthenticated user (no email)")
		client.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	allowed, proxyList, err := s.getUserProxyConfig(ctx, userEmail, userCountry)
	if err != nil {
		logError("Proxy Config API Error for User %s: %v", userEmail, err)
		client.Write([]byte{0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	if !allowed || len(proxyList) == 0 {
		logWarn("SOCKS5 BLOCKED User: %s, Country: %s", userEmail, userCountry)
		client.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}

	var activeCandidates []ProxyParams
	var failedCandidates []ProxyParams
	for _, p := range proxyList {
		if GlobalFailures.IsFailed(p.Host, p.Port) {
			failedCandidates = append(failedCandidates, p)
		} else {
			activeCandidates = append(activeCandidates, p)
		}
	}
	candidates := append(activeCandidates, failedCandidates...)

	var upstream net.Conn
	var connErr error
	var chosenProxy *ProxyParams

	for i := range candidates {
		p := &candidates[i]
		upstream, connErr = s.connectProxy(p, destAddr, destPort)
		if connErr == nil {
			chosenProxy = p
			break
		}
		logWarn("SOCKS5 Proxy %s:%d failed: %v. Marking failed.", p.Host, p.Port, connErr)
		GlobalFailures.MarkFailed(p.Host, p.Port)
	}

	if connErr != nil || chosenProxy == nil {
		logError("Upstream Error via all candidates: %v. Cache Invalidated.", connErr)
		s.authCache.Delete(userEmail)
		client.Write([]byte{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()

	if _, err := client.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}

	s.relay(client, upstream)
}

func (s *Server) readHandshake(client net.Conn) error {
	buf := make([]byte, 256)
	if _, err := io.ReadFull(client, buf[:2]); err != nil {
		return err
	}
	if buf[0] != 0x05 {
		return fmt.Errorf("unsupported socks version")
	}
	numMethods := int(buf[1])
	if _, err := io.ReadFull(client, buf[:numMethods]); err != nil {
		return err
	}

	hasNoAuth := false
	for i := 0; i < numMethods; i++ {
		if buf[i] == 0x00 {
			hasNoAuth = true
			break
		}
	}

	if !hasNoAuth {
		client.Write([]byte{0x05, 0xff})
		return fmt.Errorf("no acceptable authentication methods")
	}

	_, err := client.Write([]byte{0x05, 0x00})
	return err
}

func (s *Server) readDestination(client net.Conn) (string, uint16, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(client, buf); err != nil {
		return "", 0, err
	}
	if buf[0] != 0x05 || buf[1] != 0x01 {
		return "", 0, fmt.Errorf("unsupported socks command")
	}

	var destAddr string
	addrType := buf[3]

	switch addrType {
	case 0x01:
		ipBuf := make([]byte, 4)
		if _, err := io.ReadFull(client, ipBuf); err != nil {
			return "", 0, err
		}
		destAddr = net.IP(ipBuf).String()
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(client, lenBuf); err != nil {
			return "", 0, err
		}
		domainLen := int(lenBuf[0])
		domainBuf := make([]byte, domainLen)
		if _, err := io.ReadFull(client, domainBuf); err != nil {
			return "", 0, err
		}
		destAddr = string(domainBuf)
	case 0x04:
		ipBuf := make([]byte, 16)
		if _, err := io.ReadFull(client, ipBuf); err != nil {
			return "", 0, err
		}
		destAddr = net.IP(ipBuf).String()
	default:
		return "", 0, fmt.Errorf("unsupported address type")
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(client, portBuf); err != nil {
		return "", 0, err
	}
	destPort := binary.BigEndian.Uint16(portBuf)

	return destAddr, destPort, nil
}

type IdleTimeoutConn struct {
	net.Conn
	idleTimeout time.Duration
}

func (c *IdleTimeoutConn) Read(b []byte) (int, error) {
	if c.idleTimeout > 0 {
		c.Conn.SetReadDeadline(time.Now().Add(c.idleTimeout))
	}
	n, err := c.Conn.Read(b)
	if c.idleTimeout > 0 {
		c.Conn.SetReadDeadline(time.Time{})
	}
	return n, err
}

func (c *IdleTimeoutConn) Write(b []byte) (int, error) {
	if c.idleTimeout > 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(c.idleTimeout))
	}
	n, err := c.Conn.Write(b)
	if c.idleTimeout > 0 {
		c.Conn.SetWriteDeadline(time.Time{})
	}
	return n, err
}

func (s *Server) waitWebhook(targetKey string) (string, string) {
	entry, found := s.webhookCache.Get(targetKey)
	if found {
		return entry.Email, entry.Country
	}

	sig := &ConnectionSignal{ch: make(chan struct{})}
	actual, loaded := s.pendingConns.GetOrStore(targetKey, sig)
	if loaded {
		sig = actual
	}

	select {
	case <-sig.ch:
	case <-time.After(s.webhookTimeout):
	}

	s.pendingConns.DeleteIfMatch(targetKey, sig)

	if cachedEntry, foundCached := s.webhookCache.Get(targetKey); foundCached {
		return cachedEntry.Email, cachedEntry.Country
	}

	return "", ""
}

func (s *Server) connectProxy(proxy *ProxyParams, destAddr string, destPort uint16) (net.Conn, error) {
	proxyName := fmt.Sprintf("%s:%d", proxy.Host, proxy.Port)
	logDebug("Routing via %s (%s) to %s:%d", proxyName, proxy.Type, destAddr, destPort)
	return s.pool.Get(proxy, destAddr, destPort)
}

func (s *Server) relay(client, upstream net.Conn) {
	timeout := s.idleTimeout
	if timeout == 0 {
		timeout = 300 * time.Second
	}

	clientWrapped := &IdleTimeoutConn{Conn: client, idleTimeout: timeout}
	upstreamWrapped := &IdleTimeoutConn{Conn: upstream, idleTimeout: timeout}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(upstreamWrapped, clientWrapped)
		upstream.Close()
	}()

	go func() {
		defer wg.Done()
		io.Copy(clientWrapped, upstreamWrapped)
		client.Close()
	}()

	wg.Wait()
}
