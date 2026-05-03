package servicecore

import "time"

type HealthStatus struct {
	Service   string    `json:"service"`
	Status    string    `json:"status"`
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}

func HealthController(serviceName, version string) HealthStatus {
	return HealthStatus{
		Service:   serviceName,
		Status:    "ok",
		Version:   version,
		Timestamp: time.Now().UTC(),
	}
}
