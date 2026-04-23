package transport

import (
	"log/slog"
	"time"
)

type CloseHandler func(code int, text string) error

type ConnectionConfig struct {
	CloseHandler       CloseHandler
	WriteTimeout       time.Duration
	IncomingBufferSize int
	ErrorsBufferSize   int
	LoggingEnabled     bool
	LogLevel           *slog.Level
	Retry              *RetryConfig
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
	if err := ValidateConnectionConfig(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func GetDefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		CloseHandler:       nil,
		WriteTimeout:       30 * time.Second,
		IncomingBufferSize: 100,
		ErrorsBufferSize:   10,
		LoggingEnabled:     true,
		LogLevel:           nil,
		Retry:              GetDefaultRetryConfig(),
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

func ValidateConnectionConfig(config *ConnectionConfig) error {
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
			return NewConfigError(InvalidDelays)
		}
	}

	return nil
}

func validateWriteTimeout(value time.Duration) error {
	if value < 0 {
		return InvalidWriteTimeout
	}
	return nil
}

func validateBufferSize(value int) error {
	if value < 1 {
		return InvalidBufferSize
	}
	return nil
}

func validateMaxRetries(value int) error {
	if value < 0 {
		return InvalidRetryMaxRetries
	}
	return nil
}

func validateInitialDelay(value time.Duration) error {
	if value <= 0 {
		return InvalidRetryInitialDelay
	}
	return nil
}

func validateMaxDelay(value time.Duration) error {
	if value <= 0 {
		return InvalidRetryMaxDelay
	}
	return nil
}

func validateJitterFactor(value float64) error {
	if value < 0.0 || value > 1.0 {
		return InvalidRetryJitterFactor
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

func WithIncomingBufferSize(value int) ConnectionOption {
	return func(c *ConnectionConfig) error {
		if err := validateBufferSize(value); err != nil {
			return err
		}
		c.IncomingBufferSize = value
		return nil
	}
}

func WithErrorsBufferSize(value int) ConnectionOption {
	return func(c *ConnectionConfig) error {
		if err := validateBufferSize(value); err != nil {
			return err
		}
		c.ErrorsBufferSize = value
		return nil
	}
}

func WithLoggingEnabled(value bool) ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.LoggingEnabled = value
		return nil
	}
}

func WithLogLevel(level slog.Level) ConnectionOption {
	return func(c *ConnectionConfig) error {
		l := level
		c.LogLevel = &l
		return nil
	}
}

func WithoutRetry() ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.Retry = nil
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
