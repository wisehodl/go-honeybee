package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"time"
)

type CloseHandler func(code int, text string) error

type Config struct {
	CloseHandler CloseHandler
	WriteTimeout time.Duration
	Retry        *RetryConfig
}

type RetryConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	JitterFactor float64
}

type ConfigOption func(*Config) error

func NewConfig(options ...ConfigOption) (*Config, error) {
	conf := GetDefaultConfig()
	if err := SetConfig(conf, options...); err != nil {
		return nil, err
	}
	if err := ValidateConfig(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func GetDefaultConfig() *Config {
	return &Config{
		CloseHandler: nil,
		WriteTimeout: 30 * time.Second,
		Retry:        GetDefaultRetryConfig(),
	}
}

func GetDefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:   0, // Infinite retries
		InitialDelay: 1 * time.Second,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.5,
	}
}

func SetConfig(config *Config, options ...ConfigOption) error {
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	return nil
}

func ValidateConfig(config *Config) error {
	if config.Retry != nil {
		if config.Retry.InitialDelay > config.Retry.MaxDelay {
			return errors.NewConfigError("initial delay may not exceed maximum delay")
		}
	}

	return nil
}

// Configuration Options

func WithCloseHandler(handler CloseHandler) ConfigOption {
	return func(c *Config) error {
		c.CloseHandler = handler
		return nil
	}
}

// When WriteTimeout is set to zero, read timeouts are disabled.
func WithWriteTimeout(value time.Duration) ConfigOption {
	return func(c *Config) error {
		if value < 0 {
			return errors.InvalidWriteTimeout
		}
		c.WriteTimeout = value
		return nil
	}
}

// WithRetry enables retry with default parameters (infinite retries,
// 1s initial delay, 5s max delay, 0.5 jitter factor).
//
// If passed after granular retry options (WithRetryMaxRetries, etc.),
// it will overwrite them. Use either WithRetry alone or the granular
// options; not both.
func WithRetry() ConfigOption {
	return func(c *Config) error {
		c.Retry = GetDefaultRetryConfig()
		return nil
	}
}

func WithRetryMaxRetries(value int) ConfigOption {
	return func(c *Config) error {
		if c.Retry == nil {
			c.Retry = GetDefaultRetryConfig()
		}
		if value < 0 {
			return errors.InvalidRetryMaxRetries
		}
		c.Retry.MaxRetries = value
		return nil
	}
}

func WithRetryInitialDelay(value time.Duration) ConfigOption {
	return func(c *Config) error {
		if c.Retry == nil {
			c.Retry = GetDefaultRetryConfig()
		}
		if value <= 0 {
			return errors.InvalidRetryInitialDelay
		}
		c.Retry.InitialDelay = value
		return nil
	}
}

func WithRetryMaxDelay(value time.Duration) ConfigOption {
	return func(c *Config) error {
		if c.Retry == nil {
			c.Retry = GetDefaultRetryConfig()
		}
		if value <= 0 {
			return errors.InvalidRetryMaxDelay
		}
		c.Retry.MaxDelay = value
		return nil
	}
}

func WithRetryJitterFactor(value float64) ConfigOption {
	return func(c *Config) error {
		if c.Retry == nil {
			c.Retry = GetDefaultRetryConfig()
		}
		if value < 0.0 || value > 1.0 {
			return errors.InvalidRetryJitterFactor
		}
		c.Retry.JitterFactor = value
		return nil
	}
}
