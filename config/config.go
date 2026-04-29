package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App    AppConfig    `yaml:"app"`
	DB     DBConfig     `yaml:"db"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	NATS   NATSConfig   `yaml:"nats"`
	Redis  RedisConfig  `yaml:"redis"`
	Server ServerConfig `yaml:"server"`
}

type AppConfig struct {
	Name string `yaml:"name"`
	Env  string `yaml:"env"`
}

type DBConfig struct {
	Host                  string `yaml:"host"`
	Port                  int    `yaml:"port"`
	User                  string `yaml:"user"`
	Password              string `yaml:"password"`
	Name                  string `yaml:"name"`
	SSLMode               string `yaml:"sslmode"`
	MaxOpenConns          int    `yaml:"max_open_conns"`
	MaxIdleConns          int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeMinute int    `yaml:"conn_max_lifetime_minutes"`
}

type KafkaConfig struct {
	Brokers        []string `yaml:"brokers"`
	ClientID       string   `yaml:"client_id"`
	TopicUserPoint string   `yaml:"topic_user_point"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type NATSConfig struct {
	URL     string `yaml:"url"`
	Stream  string `yaml:"stream"`
	Subject string `yaml:"subject"`
	Durable string `yaml:"durable"`
	Enabled bool   `yaml:"enabled"`
}

type ServerConfig struct {
	Port               int `yaml:"port"`
	ReadTimeoutSec     int `yaml:"read_timeout_seconds"`
	WriteTimeoutSec    int `yaml:"write_timeout_seconds"`
	IdleTimeoutSec     int `yaml:"idle_timeout_seconds"`
	ShutdownTimeoutSec int `yaml:"shutdown_timeout_seconds"`
}

func Load(path string) (*Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(file, &cfg); err != nil {
		return nil, err
	}

	cfg.applyEnvOverrides()
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.App.Name == "" {
		return fmt.Errorf("app.name is required")
	}
	if c.DB.Host == "" || c.DB.User == "" || c.DB.Name == "" {
		return fmt.Errorf("db host/user/name are required")
	}
	if c.DB.Port <= 0 {
		return fmt.Errorf("db.port must be greater than zero")
	}
	if c.Server.Port <= 0 {
		return fmt.Errorf("server.port must be greater than zero")
	}
	if c.DB.MaxOpenConns < c.DB.MaxIdleConns {
		return fmt.Errorf("db.max_open_conns must be >= db.max_idle_conns")
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Server.ReadTimeoutSec <= 0 {
		c.Server.ReadTimeoutSec = 10
	}
	if c.Server.WriteTimeoutSec <= 0 {
		c.Server.WriteTimeoutSec = 10
	}
	if c.Server.IdleTimeoutSec <= 0 {
		c.Server.IdleTimeoutSec = 60
	}
	if c.Server.ShutdownTimeoutSec <= 0 {
		c.Server.ShutdownTimeoutSec = 10
	}
	if c.DB.MaxOpenConns <= 0 {
		c.DB.MaxOpenConns = 25
	}
	if c.DB.MaxIdleConns <= 0 {
		c.DB.MaxIdleConns = 10
	}
	if c.DB.ConnMaxLifetimeMinute <= 0 {
		c.DB.ConnMaxLifetimeMinute = 30
	}
	if c.DB.SSLMode == "" {
		c.DB.SSLMode = "disable"
	}
	if c.NATS.URL == "" {
		c.NATS.URL = "nats://localhost:4222"
	}
	if c.NATS.Stream == "" {
		c.NATS.Stream = "ADMIN_EVENTS"
	}
	if c.NATS.Subject == "" {
		c.NATS.Subject = "admin.promotion.*"
	}
	if c.NATS.Durable == "" {
		c.NATS.Durable = "promotion-admin-consumer"
	}
}

func (c *Config) applyEnvOverrides() {
	overrideString(&c.App.Name, "APP_NAME")
	overrideString(&c.App.Env, "APP_ENV")
	overrideString(&c.DB.Host, "DB_HOST")
	overrideInt(&c.DB.Port, "DB_PORT")
	overrideString(&c.DB.User, "DB_USER")
	overrideString(&c.DB.Password, "DB_PASSWORD")
	overrideString(&c.DB.Name, "DB_NAME")
	overrideString(&c.DB.SSLMode, "DB_SSLMODE")
	overrideInt(&c.DB.MaxOpenConns, "DB_MAX_OPEN_CONNS")
	overrideInt(&c.DB.MaxIdleConns, "DB_MAX_IDLE_CONNS")
	overrideInt(&c.DB.ConnMaxLifetimeMinute, "DB_CONN_MAX_LIFETIME_MINUTES")
	overrideInt(&c.Server.Port, "SERVER_PORT")
	overrideInt(&c.Server.ReadTimeoutSec, "SERVER_READ_TIMEOUT_SECONDS")
	overrideInt(&c.Server.WriteTimeoutSec, "SERVER_WRITE_TIMEOUT_SECONDS")
	overrideInt(&c.Server.IdleTimeoutSec, "SERVER_IDLE_TIMEOUT_SECONDS")
	overrideInt(&c.Server.ShutdownTimeoutSec, "SERVER_SHUTDOWN_TIMEOUT_SECONDS")
	overrideString(&c.NATS.URL, "NATS_URL")
	overrideString(&c.NATS.Stream, "NATS_STREAM")
	overrideString(&c.NATS.Subject, "NATS_SUBJECT")
	overrideString(&c.NATS.Durable, "NATS_DURABLE")

	if brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); brokers != "" {
		c.Kafka.Brokers = strings.Split(brokers, ",")
	}
}

func overrideString(dest *string, envName string) {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		*dest = v
	}
}

func overrideInt(dest *int, envName string) {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			*dest = parsed
		}
	}
}
