package transport

import (
	"git.wisehodl.dev/jay/go-honeybee/types"
	"net/http"
	"time"
)

// ----------------------------------------------------------------------------
// Connection Config
// ----------------------------------------------------------------------------

// Types

type CloseHandler func(code int, text string) error

type ConnectionConfig struct {
	CloseHandler       CloseHandler
	RequestHeader      http.Header
	WriteTimeout       time.Duration
	PingInterval       time.Duration
	IncomingBufferSize int
	ErrorsBufferSize   int
	Retry              RetryConfig
	Dialer             types.Dialer
}

type RetryConfig struct {
	Disabled     bool
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	JitterFactor float64
}

type ConnectionOption func(*ConnectionConfig) error

// Constructors

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
	header := http.Header{}
	header.Set("User-Agent", "honeybee/0.1.0")
	return &ConnectionConfig{
		CloseHandler:       nil,
		RequestHeader:      header,
		WriteTimeout:       30 * time.Second,
		PingInterval:       20 * time.Second,
		IncomingBufferSize: 100,
		ErrorsBufferSize:   10,
		Retry: RetryConfig{
			MaxRetries:   0, // Infinite retries
			InitialDelay: 1 * time.Second,
			MaxDelay:     60 * time.Second,
			JitterFactor: 0.2,
		},
	}
}

func (c ConnectionConfig) Clone() ConnectionConfig {
	if c.RequestHeader != nil {
		c.RequestHeader = c.RequestHeader.Clone()
	}
	return c
}

func applyConnectionOptions(config *ConnectionConfig, options ...ConnectionOption) error {
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	return nil
}

// Validation

func ValidateConnectionConfig(config *ConnectionConfig) error {
	err := validateWriteTimeout(config.WriteTimeout)
	if err != nil {
		return err
	}

	if !config.Retry.Disabled {
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

func validatePingInterval(value time.Duration) error {
	if value < 0 {
		return InvalidPingInterval
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

// Options

func WithCloseHandler(handler CloseHandler) ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.CloseHandler = handler
		return nil
	}
}

func WithRequestHeader(header http.Header) ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.RequestHeader = header.Clone()
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

// When PingInterval is set to zero, ping frames are disabled.
func WithPingInterval(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) error {
		err := validatePingInterval(value)
		if err != nil {
			return err
		}
		c.PingInterval = value
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

func WithConnectionDialer(d types.Dialer) ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.Dialer = d
		return nil
	}
}

func WithRetryDisabled() ConnectionOption {
	return func(c *ConnectionConfig) error {
		c.Retry.Disabled = true
		return nil
	}
}

func WithRetryMaxRetries(value int) ConnectionOption {
	return func(c *ConnectionConfig) error {
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
		err := validateJitterFactor(value)
		if err != nil {
			return err
		}

		c.Retry.JitterFactor = value
		return nil
	}
}
