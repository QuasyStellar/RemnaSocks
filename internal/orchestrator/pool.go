package orchestrator

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BufferedConn struct {
	net.Conn
	Reader io.Reader
}

func (c *BufferedConn) Read(b []byte) (int, error) {
	return c.Reader.Read(b)
}

type PooledConn struct {
	conn      net.Conn
	createdAt time.Time
}

type ProxyBucket struct {
	mu         sync.Mutex
	ch         chan PooledConn
	lastActive time.Time
	dialing    int32
}

type ProxyPool struct {
	mu      sync.RWMutex
	buckets map[string]*ProxyBucket
}

func NewProxyPool() *ProxyPool {
	p := &ProxyPool{
		buckets: make(map[string]*ProxyBucket),
	}
	go p.cleanupWorker()
	return p
}

func (p *ProxyPool) getBucket(key string) *ProxyBucket {
	p.mu.RLock()
	b, exists := p.buckets[key]
	p.mu.RUnlock()
	if exists {
		return b
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	b, exists = p.buckets[key]
	if !exists {
		b = &ProxyBucket{
			ch:         make(chan PooledConn, 3),
			lastActive: time.Now(),
		}
		p.buckets[key] = b
	}
	return b
}

func (p *ProxyPool) cleanupWorker() {
	for {
		time.Sleep(5 * time.Second)
		now := time.Now()

		p.mu.RLock()
		buckets := make([]*ProxyBucket, 0, len(p.buckets))
		for _, b := range p.buckets {
			buckets = append(buckets, b)
		}
		p.mu.RUnlock()

		for _, b := range buckets {
			b.mu.Lock()
			lastActive := b.lastActive
			b.mu.Unlock()

			size := len(b.ch)
			for i := 0; i < size; i++ {
				select {
				case pc := <-b.ch:
					if now.Sub(pc.createdAt) > 10*time.Second || now.Sub(lastActive) > 60*time.Second {
						pc.conn.Close()
					} else {
						select {
						case b.ch <- pc:
						default:
							pc.conn.Close()
						}
					}
				default:
				}
			}
		}
	}
}

func isConnAlive(conn net.Conn) bool {
	conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	one := make([]byte, 1)
	_, err := conn.Read(one)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return true
		}
		return false
	}
	return false
}

func (p *ProxyPool) Get(proxy *ProxyParams, targetHost string, targetPort uint16) (net.Conn, error) {
	key := fmt.Sprintf("%s:%s:%d:%s:%s", proxy.Type, proxy.Host, proxy.Port, proxy.User, proxy.Pass)
	b := p.getBucket(key)

	b.mu.Lock()
	b.lastActive = time.Now()
	b.mu.Unlock()

	for {
		select {
		case pc := <-b.ch:
			if time.Since(pc.createdAt) < 10*time.Second && isConnAlive(pc.conn) {
				go p.replenish(proxy, b)
				return p.finalizeConnection(pc.conn, proxy, targetHost, targetPort)
			}
			pc.conn.Close()
		default:
			go p.replenish(proxy, b)
			return p.dialAndEstablishProxy(proxy, targetHost, targetPort)
		}
	}
}

func (p *ProxyPool) replenish(proxy *ProxyParams, b *ProxyBucket) {
	b.mu.Lock()
	lastUse := b.lastActive
	if time.Since(lastUse) > 30*time.Second {
		b.mu.Unlock()
		return
	}

	if atomic.LoadInt32(&b.dialing) >= 1 {
		b.mu.Unlock()
		return
	}

	if len(b.ch)+int(atomic.LoadInt32(&b.dialing)) >= cap(b.ch) {
		b.mu.Unlock()
		return
	}

	atomic.AddInt32(&b.dialing, 1)
	b.mu.Unlock()

	defer atomic.AddInt32(&b.dialing, -1)

	conn, err := prewarmConnection(proxy)
	if err != nil {
		return
	}

	select {
	case b.ch <- PooledConn{conn: conn, createdAt: time.Now()}:
	default:
		conn.Close()
	}
}

func prewarmConnection(proxy *ProxyParams) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", proxy.Host, proxy.Port), 5*time.Second)
	if err != nil {
		return nil, err
	}

	if strings.ToLower(proxy.Type) == "http" {
		return conn, nil
	}

	var handshakePayload []byte
	isAuthRequired := proxy.User != "" && proxy.Pass != ""

	if !isAuthRequired {
		handshakePayload = []byte{0x05, 0x01, 0x00}
	} else {
		handshakePayload = []byte{0x05, 0x01, 0x02}
		userBytes := []byte(proxy.User)
		passBytes := []byte(proxy.Pass)
		authReq := make([]byte, 0, 2+len(userBytes)+1+len(passBytes))
		authReq = append(authReq, 0x01, byte(len(userBytes)))
		authReq = append(authReq, userBytes...)
		authReq = append(authReq, byte(len(passBytes)))
		authReq = append(authReq, passBytes...)
		handshakePayload = append(handshakePayload, authReq...)
	}

	if _, err := conn.Write(handshakePayload); err != nil {
		conn.Close()
		return nil, err
	}

	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}
	if resp[0] != 0x05 {
		conn.Close()
		return nil, fmt.Errorf("invalid socks version")
	}

	selectedMethod := resp[1]
	if selectedMethod == 0x02 {
		if !isAuthRequired {
			conn.Close()
			return nil, fmt.Errorf("proxy requires auth but none provided")
		}
		authResp := make([]byte, 2)
		if _, err := io.ReadFull(conn, authResp); err != nil {
			conn.Close()
			return nil, err
		}
		if authResp[0] != 0x01 || authResp[1] != 0x00 {
			conn.Close()
			return nil, fmt.Errorf("authentication failed")
		}
	} else if selectedMethod != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("unsupported authentication method: %d", selectedMethod)
	}

	return conn, nil
}

func (p *ProxyPool) finalizeConnection(conn net.Conn, proxy *ProxyParams, targetHost string, targetPort uint16) (net.Conn, error) {
	if strings.ToLower(proxy.Type) == "http" {
		req := fmt.Sprintf("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n", targetHost, targetPort, targetHost, targetPort)
		if proxy.User != "" && proxy.Pass != "" {
			auth := proxy.User + ":" + proxy.Pass
			basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
			req += "Proxy-Authorization: " + basicAuth + "\r\n"
		}
		req += "\r\n"

		if _, err := conn.Write([]byte(req)); err != nil {
			conn.Close()
			return nil, err
		}

		reader := bufio.NewReader(conn)
		respLine, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, err
		}

		if !strings.Contains(respLine, "200") {
			conn.Close()
			return nil, fmt.Errorf("http proxy rejected connection: %s", strings.TrimSpace(respLine))
		}

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				conn.Close()
				return nil, err
			}
			if line == "\r\n" || line == "\n" {
				break
			}
		}

		return &BufferedConn{Conn: conn, Reader: reader}, nil
	}

	var addrPayload []byte
	var addrType byte

	if ip := net.ParseIP(targetHost); ip != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			addrType = 0x01
			addrPayload = ipv4
		} else {
			addrType = 0x04
			addrPayload = ip.To16()
		}
	} else {
		addrType = 0x03
		domainBytes := []byte(targetHost)
		addrPayload = make([]byte, 0, 1+len(domainBytes))
		addrPayload = append(addrPayload, byte(len(domainBytes)))
		addrPayload = append(addrPayload, domainBytes...)
	}

	req := make([]byte, 0, 4+len(addrPayload)+2)
	req = append(req, 0x05, 0x01, 0x00, addrType)
	req = append(req, addrPayload...)
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, targetPort)
	req = append(req, portBuf...)

	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, err
	}

	respHdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, respHdr); err != nil {
		conn.Close()
		return nil, err
	}
	if respHdr[0] != 0x05 || respHdr[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("connection rejected, status: %d", respHdr[1])
	}

	repAddrType := respHdr[3]
	switch repAddrType {
	case 0x01:
		junk := make([]byte, 6)
		if _, err := io.ReadFull(conn, junk); err != nil {
			conn.Close()
			return nil, err
		}
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			conn.Close()
			return nil, err
		}
		junk := make([]byte, int(lenBuf[0])+2)
		if _, err := io.ReadFull(conn, junk); err != nil {
			conn.Close()
			return nil, err
		}
	case 0x04:
		junk := make([]byte, 18)
		if _, err := io.ReadFull(conn, junk); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

func (p *ProxyPool) dialAndEstablishProxy(proxy *ProxyParams, targetHost string, targetPort uint16) (net.Conn, error) {
	conn, err := prewarmConnection(proxy)
	if err != nil {
		return nil, err
	}
	return p.finalizeConnection(conn, proxy, targetHost, targetPort)
}
