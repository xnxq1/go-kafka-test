package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	ServerAddr         string        `env:"SERVER_ADDR" envDefault:":8080"`
	ServerReadTimeout  time.Duration `env:"SERVER_READ_TIMEOUT" envDefault:"10s"`
	ServerWriteTimeout time.Duration `env:"SERVER_WRITE_TIMEOUT" envDefault:"30s"`

	DatabaseURL         string `env:"DATABASE_URL" envDefault:"postgres://postgres:postgres@localhost:5432/messages"`
	DBMaxConns          int32  `env:"DB_MAX_CONNS" envDefault:"5"`
	DBMinConns          int32  `env:"DB_MIN_CONNS" envDefault:"1"`
	MaxRetryOutboxCount int    `env:"MAX_RETRY_OUTBOX_COUNT" envDefault:"5"`
	Limit               int    `env:"LIMIT" envDefault:"100"`
	OutboxLimit         int    `env:"OUTBOX_LIMIT" envDefault:"100"`
	OutboxDelay         int    `env:"OUTBOX_DELAY" envDefault:"5"`
}

func (c *Config) GetOutboxMaxRetryCount() int {
	return c.MaxRetryOutboxCount
}
func (c *Config) GetLimit() int {
	return c.Limit
}

func (c *Config) GetOutboxLimit() int {
	return c.OutboxLimit
}
func LoadConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
