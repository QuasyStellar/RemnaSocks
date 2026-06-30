package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type UserResponse struct {
	Response struct {
		Description          string `json:"description"`
		ActiveInternalSquads []struct {
			Name string `json:"name"`
		} `json:"activeInternalSquads"`
	} `json:"response"`
}

func (s *Server) getUserProxyConfigInternal(ctx context.Context, username string, country string, checkSquad bool) (bool, ProxyList, error) {
	if username == "" {
		return false, nil, nil
	}

	if s.panelURL == "" || s.panelToken == "" {
		logError("PANEL_URL or PANEL_TOKEN is not set.")
		return false, nil, nil
	}

	entry, found := s.authCache.Get(username)
	if found && time.Now().Before(entry.ExpiresAt) {
		return entry.IsAllowed, s.selectProxy(entry.MultiProxy, entry.SingleProxy, country), nil
	}

	reqURL := s.buildReqURL(username)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return false, nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.panelToken))
	req.Header.Set("Accept", "application/json")
	if s.panelCookie != "" {
		req.Header.Set("Cookie", s.panelCookie)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		if found {
			return entry.IsAllowed, s.selectProxy(entry.MultiProxy, entry.SingleProxy, country), nil
		}
		return false, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if found {
			return entry.IsAllowed, s.selectProxy(entry.MultiProxy, entry.SingleProxy, country), nil
		}
		s.authCache.Set(username, AuthCacheEntry{
			IsAllowed: false,
			ExpiresAt: time.Now().Add(s.unauthTTL),
		})
		return false, nil, nil
	}

	var userResp UserResponse
	if err := json.NewDecoder(resp.Body).Decode(&userResp); err != nil {
		return false, nil, err
	}

	isAllowed := s.checkUserAllowed(userResp, checkSquad)
	multiProxy, singleProxy := s.parseProxyDescription(userResp.Response.Description)

	ttl := s.unauthTTL
	if isAllowed {
		ttl = s.authTTL
	}

	s.authCache.Set(username, AuthCacheEntry{
		IsAllowed:   isAllowed,
		MultiProxy:  multiProxy,
		SingleProxy: singleProxy,
		ExpiresAt:   time.Now().Add(ttl),
	})

	if !isAllowed {
		return false, nil, nil
	}

	return true, s.selectProxy(multiProxy, singleProxy, country), nil
}

func (s *Server) buildReqURL(username string) string {
	if _, err := strconv.Atoi(username); err == nil {
		return fmt.Sprintf("%s/api/users/by-id/%s", s.panelURL, url.PathEscape(username))
	}
	return fmt.Sprintf("%s/api/users/by-username/%s", s.panelURL, url.PathEscape(username))
}

func (s *Server) checkUserAllowed(userResp UserResponse, checkSquad bool) bool {
	if s.allowedSquad == "" || !checkSquad {
		return true
	}
	for _, squad := range userResp.Response.ActiveInternalSquads {
		if squad.Name == s.allowedSquad {
			return true
		}
	}
	return false
}

func (s *Server) parseProxyDescription(desc string) (map[string]ProxyList, ProxyList) {
	if desc == "" {
		return nil, nil
	}

	var multi map[string]ProxyList
	if err := json.Unmarshal([]byte(desc), &multi); err == nil && len(multi) > 0 {
		return multi, nil
	}

	var list ProxyList
	if err := json.Unmarshal([]byte(desc), &list); err == nil && len(list) > 0 {
		return nil, list
	}

	var single ProxyParams
	if err := json.Unmarshal([]byte(desc), &single); err == nil && single.Host != "" {
		return nil, []ProxyParams{single}
	}

	return nil, nil
}

func (s *Server) selectProxy(multiProxy map[string]ProxyList, singleProxy ProxyList, country string) ProxyList {
	if multiProxy != nil {
		if list, ok := multiProxy[country]; ok && len(list) > 0 {
			return list
		}
		return nil
	}
	return singleProxy
}

func (s *Server) getUserProxyConfig(ctx context.Context, username string, country string) (bool, ProxyList, error) {
	if username != "" {
		allowed, proxy, err := s.getUserProxyConfigInternal(ctx, username, country, true)
		if err != nil {
			return false, nil, err
		}
		if allowed && len(proxy) > 0 {
			return true, proxy, nil
		}
	}

	if country != "" {
		allowed, proxy, err := s.getUserProxyConfigInternal(ctx, country, country, false)
		if err != nil {
			return false, nil, err
		}
		if allowed && len(proxy) > 0 {
			return true, proxy, nil
		}
	}

	return false, nil, nil
}
