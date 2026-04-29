package config

import "testing"

func TestConfigValidate(t *testing.T) {
	cfg := &Config{
		App: AppConfig{Name: "promotion-service", Env: "dev"},
		DB: DBConfig{
			Host:                  "localhost",
			Port:                  5432,
			User:                  "user",
			Name:                  "db",
			SSLMode:               "disable",
			MaxOpenConns:          10,
			MaxIdleConns:          5,
			ConnMaxLifetimeMinute: 30,
		},
		Server: ServerConfig{Port: 8080},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected config valid, got error: %v", err)
	}
}

func TestConfigValidateFailWhenPortInvalid(t *testing.T) {
	cfg := &Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error")
	}
}
