package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"time"
)

// Types

type CloseHandler func(code int, text string) error

// Pool Config

type PoolConfig struct {
	ConnectionConfig *ConnectionConfig
}

// Connection Config

type ConnectionConfig struct {
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

type ConnectionOption func(*ConnectionConfig) error

func NewConnectionConfig(options ...ConnectionOption) (*ConnectionConfig, error) {
	conf := GetDefaultConnectionConfig()
	if err := applyConnectionOptions(conf, options...); err != nil {
		return nil, err
	}
	if err := validateConnectionConfig(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func GetDefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
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

func applyConnectionOptions(config *ConnectionConfig, options ...ConnectionOption) error {
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	return nil
}

func validateConnectionConfig(config *ConnectionConfig) error {
	if config.Retry != nil {
		if config.Retry.InitialDelay > config.Retry.MaxDelay {
			return errors.NewConfigError("initial delay may not exceed maximum delay")
		}
	}

	return nil
}

// Configuration Options

func WithCloseHandler(handler CloseHandler) ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.CloseHandler = handler
		return nil
	}
}

// When WriteTimeout is set to zero, read timeouts are disabled.
func WithWriteTimeout(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) error {
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
func WithRetry() ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.Retry = GetDefaultRetryConfig()
		return nil
	}
}

func WithRetryMaxRetries(value int) ConnectionOption {
	return func(c *ConnectionConfig) error {
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

func WithRetryInitialDelay(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) error {
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

func WithRetryMaxDelay(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) error {
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

func WithRetryJitterFactor(value float64) ConnectionOption {
	return func(c *ConnectionConfig) error {
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
