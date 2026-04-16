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
	IdleTimeout      time.Duration
}

type PoolOption func(*PoolConfig) error

func NewPoolConfig(options ...PoolOption) (*PoolConfig, error) {
	conf := GetDefaultPoolConfig()
	if err := applyPoolOptions(conf, options...); err != nil {
		return nil, err
	}
	if err := validatePoolConfig(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func GetDefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		IdleTimeout:      20 * time.Second,
		ConnectionConfig: nil,
	}
}

func applyPoolOptions(config *PoolConfig, options ...PoolOption) error {
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	return nil
}

func validatePoolConfig(config *PoolConfig) error {
	err := validateIdleTimeout(config.IdleTimeout)
	if err != nil {
		return err
	}

	if config.ConnectionConfig != nil {
		err = validateConnectionConfig(config.ConnectionConfig)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateIdleTimeout(value time.Duration) error {
	if value < 0 {
		return errors.InvalidIdleTimeout
	}
	return nil
}

// When IdleTimeout is set to zero, idle timeouts are disabled.
func WithIdleTimeout(value time.Duration) PoolOption {
	return func(c *PoolConfig) error {
		err := validateIdleTimeout(value)
		if err != nil {
			return err
		}
		c.IdleTimeout = value
		return nil
	}
}

func WithConnectionConfig(cc *ConnectionConfig) PoolOption {
	return func(c *PoolConfig) error {
		err := validateConnectionConfig(cc)
		if err != nil {
			return err
		}
		c.ConnectionConfig = cc
		return nil
	}
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
	err := validateWriteTimeout(config.WriteTimeout)
	if err != nil {
		return err
	}

	if config.Retry != nil {
		err = validateMaxRetries(config.Retry.MaxRetries)
		if err != nil {
			return err
		}

		err = validateInitialDelay(config.Retry.InitialDelay)
		if err != nil {
			return err
		}

		err = validateMaxDelay(config.Retry.MaxDelay)
		if err != nil {
			return err
		}

		err = validateJitterFactor(config.Retry.JitterFactor)
		if err != nil {
			return err
		}

		if config.Retry.InitialDelay > config.Retry.MaxDelay {
			return errors.NewConfigError("initial delay may not exceed maximum delay")
		}
	}

	return nil
}

func validateWriteTimeout(value time.Duration) error {
	if value < 0 {
		return errors.InvalidWriteTimeout
	}
	return nil
}

func validateMaxRetries(value int) error {
	if value < 0 {
		return errors.InvalidRetryMaxRetries
	}
	return nil
}

func validateInitialDelay(value time.Duration) error {
	if value <= 0 {
		return errors.InvalidRetryInitialDelay
	}
	return nil
}

func validateMaxDelay(value time.Duration) error {
	if value <= 0 {
		return errors.InvalidRetryMaxDelay
	}
	return nil
}

func validateJitterFactor(value float64) error {
	if value < 0.0 || value > 1.0 {
		return errors.InvalidRetryJitterFactor
	}
	return nil
}

func WithCloseHandler(handler CloseHandler) ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.CloseHandler = handler
		return nil
	}
}

// When WriteTimeout is set to zero, read timeouts are disabled.
func WithWriteTimeout(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) error {
		err := validateWriteTimeout(value)
		if err != nil {
			return err
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

		err := validateMaxRetries(value)
		if err != nil {
			return err
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

		err := validateInitialDelay(value)
		if err != nil {
			return err
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

		err := validateMaxDelay(value)
		if err != nil {
			return err
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

		err := validateJitterFactor(value)
		if err != nil {
			return err
		}

		c.Retry.JitterFactor = value
		return nil
	}
}
