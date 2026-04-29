package natsinfra

import (
	"time"

	"github.com/nats-io/nats.go"
)

type Config struct {
	URL           string
	MaxReconnect  int
	ReconnectWait time.Duration
	ClientName    string
}

func NewConnection(cfg Config) (*nats.Conn, error) {
	return nats.Connect(
		cfg.URL,
		nats.MaxReconnects(cfg.MaxReconnect),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.Name(cfg.ClientName),
	)
}
