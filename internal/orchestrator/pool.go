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

type ProxyPool struct {
	mu         sync.Mutex
	conns      map[string]chan PooledConn
	lastActive map[string]time.Time
}

func NewProxyPool() *ProxyPool {
	p := &ProxyPool{
		conns:      make(map[string]chan PooledConn),
		lastActive: make(map[string]time.Time),
	}
	go p.cleanupWorker()
	return p
}

func (p *ProxyPool) cleanupWorker() {
	for {
		time.Sleep(5 * time.Second)
		p.mu.Lock()
		now := time.Now()
		for key, ch := range p.conns {
			size := len(ch)
			for i := 0; i < size; i++ {
				select {
				case pc := <-ch:
					if now.Sub(pc.createdAt) > 10*time.Second {
						pc.conn.Close()
					} else {
						select {
						case ch <- pc:
						default:
							pc.conn.Close()
						}
					}
				default:
				}
			}

			lastUse, exists := p.lastActive[key]
			if exists && now.Sub(lastUse) > 60*time.Second {
				closeChan := p.conns[key]
				delete(p.conns, key)
				delete(p.lastActive, key)
				go func(c chan PooledConn) {
					for {
						select {
						case pc := <-c:
							if pc.conn != nil {
								pc.conn.Close()
							}
						default:
							return
						}
					}
				}(closeChan)
			}
		}
		p.mu.Unlock()
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

	p.mu.Lock()
	p.lastActive[key] = time.Now()
	ch, exists := p.conns[key]
	if !exists {
		ch = make(chan PooledConn, 3)
		p.conns[key] = ch
	}
	p.mu.Unlock()

	for {
		select {
		case pc := <-ch:
			if isConnAlive(pc.conn) {
				go p.replenish(proxy, key, ch)
				return p.finalizeConnection(pc.conn, proxy, targetHost, targetPort)
			}
			pc.conn.Close()
		default:
			return p.dialAndEstablishProxy(proxy, targetHost, targetPort)
		}
	}
}

func (p *ProxyPool) replenish(proxy *ProxyParams, key string, ch chan PooledConn) {
	p.mu.Lock()
	lastUse := p.lastActive[key]
	p.mu.Unlock()

	if time.Since(lastUse) > 30*time.Second {
		return
	}

	if len(ch) >= cap(ch) {
		return
	}

	conn, err := prewarmConnection(proxy)
	if err != nil {
		return
	}

	select {
	case ch <- PooledConn{conn: conn, createdAt: time.Now()}:
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
		io.ReadFull(conn, junk)
	case 0x03:
		lenBuf := make([]byte, 1)
		io.ReadFull(conn, lenBuf)
		junk := make([]byte, int(lenBuf[0])+2)
		io.ReadFull(conn, junk)
	case 0x04:
		junk := make([]byte, 18)
		io.ReadFull(conn, junk)
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
