package natsinfra

import "github.com/nats-io/nats.go"

type StreamConfig struct {
	Name     string
	Subjects []string
	Replicas int
}

func SetupJetStream(nc *nats.Conn, cfg StreamConfig) (nats.JetStreamContext, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
		Storage:  nats.FileStorage,
		Replicas: cfg.Replicas,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return nil, err
	}
	return js, nil
}
