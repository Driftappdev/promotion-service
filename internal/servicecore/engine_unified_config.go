package servicecore

import (
	"os"
	"strconv"
	"time"
)

// LoadEngineUnifiedConfigFromEnv provides a shared env mapping for all services.
func LoadEngineUnifiedConfigFromEnv(serviceName string) EngineUnifiedConfig {
	timeout := 30 * time.Second
	if raw := os.Getenv("ENGINE_TIMEOUT_SECONDS"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			timeout = time.Duration(v) * time.Second
		}
	}

	return EngineUnifiedConfig{
		ServiceName: serviceName,
		Timeout:     timeout,
	}
}
