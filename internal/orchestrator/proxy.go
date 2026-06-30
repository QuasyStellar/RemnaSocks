package orchestrator
import (
	"encoding/json"
	"fmt"
)
type ProxyParams struct {
	Type string `json:"type"`
	Host string `json:"host"`
	Port uint16 `json:"port"`
	User string `json:"user"`
	Pass string `json:"pass"`
}

type ProxyList []ProxyParams

func (pl *ProxyList) UnmarshalJSON(data []byte) error {
	var single ProxyParams
	if err := json.Unmarshal(data, &single); err == nil && single.Host != "" {
		*pl = []ProxyParams{single}
		return nil
	}

	var list []ProxyParams
	if err := json.Unmarshal(data, &list); err == nil {
		*pl = list
		return nil
	}

	return fmt.Errorf("failed to unmarshal ProxyList")
}
