package honeybee

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"log/slog"
	"time"
)

// Types

type WorkerFactory func(
	ctx context.Context,
	id string,
	logger *slog.Logger,
) (Worker, error)

// Pool Config

type PoolConfig struct {
	InboxBufferSize  int
	EventsBufferSize int
	LoggingEnabled   bool
	LogLevel         *slog.Level
	ConnectionConfig *transport.ConnectionConfig
	WorkerFactory    WorkerFactory
	WorkerConfig     *WorkerConfig
}

type PoolOption func(*PoolConfig) error

func NewPoolConfig(options ...PoolOption) (*PoolConfig, error) {
	conf := GetDefaultPoolConfig()
	if err := applyPoolOptions(conf, options...); err != nil {
		return nil, err
	}
	if err := ValidatePoolConfig(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func GetDefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		LoggingEnabled:   true,
		LogLevel:         nil,
		ConnectionConfig: nil,
		WorkerFactory:    nil,
		WorkerConfig:     nil,
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

func ValidatePoolConfig(config *PoolConfig) error {
	var err error

	if config.ConnectionConfig != nil {
		err = transport.ValidateConnectionConfig(config.ConnectionConfig)
		if err != nil {
			return err
		}
	}

	if config.WorkerConfig != nil {
		err = ValidateWorkerConfig(config.WorkerConfig)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateBufferSize(value int) error {
	if value < 1 {
		return InvalidBufferSize
	}
	return nil
}

func WithInboxBufferSize(value int) PoolOption {
	return func(c *PoolConfig) error {
		if err := validateBufferSize(value); err != nil {
			return err
		}
		c.InboxBufferSize = value
		return nil
	}
}

func WithEventsBufferSize(value int) PoolOption {
	return func(c *PoolConfig) error {
		if err := validateBufferSize(value); err != nil {
			return err
		}
		c.EventsBufferSize = value
		return nil
	}
}

func WithPoolLoggingEnabled(value bool) PoolOption {
	return func(c *PoolConfig) error {
		c.LoggingEnabled = value
		return nil
	}
}

func WithPoolLogLevel(level slog.Level) PoolOption {
	return func(c *PoolConfig) error {
		l := level
		c.LogLevel = &l
		return nil
	}
}

func WithConnectionConfig(cc *transport.ConnectionConfig) PoolOption {
	return func(c *PoolConfig) error {
		err := transport.ValidateConnectionConfig(cc)
		if err != nil {
			return err
		}
		c.ConnectionConfig = cc
		return nil
	}
}

func WithWorkerConfig(wc *WorkerConfig) PoolOption {
	return func(c *PoolConfig) error {
		err := ValidateWorkerConfig(wc)
		if err != nil {
			return err
		}
		c.WorkerConfig = wc
		return nil
	}
}

func WithWorkerFactory(wf WorkerFactory) PoolOption {
	return func(c *PoolConfig) error {
		c.WorkerFactory = wf
		return nil
	}
}

// Worker Config

type WorkerConfig struct {
	KeepaliveTimeout time.Duration
	ReconnectDelay   time.Duration
	MaxQueueSize     int
	LoggingEnabled   bool
	LogLevel         *slog.Level
}

type WorkerOption func(*WorkerConfig) error

func NewWorkerConfig(options ...WorkerOption) (*WorkerConfig, error) {
	conf := GetDefaultWorkerConfig()
	if err := applyWorkerOptions(conf, options...); err != nil {
		return nil, err
	}
	if err := ValidateWorkerConfig(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func GetDefaultWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		KeepaliveTimeout: 60 * time.Second,
		ReconnectDelay:   2 * time.Second,
		MaxQueueSize:     0, // disabled by default
		LoggingEnabled:   true,
		LogLevel:         nil,
	}
}

func applyWorkerOptions(config *WorkerConfig, options ...WorkerOption) error {
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	return nil
}

func ValidateWorkerConfig(config *WorkerConfig) error {
	err := validateKeepaliveTimeout(config.KeepaliveTimeout)
	if err != nil {
		return err
	}

	err = validateMaxQueueSize(config.MaxQueueSize)
	if err != nil {
		return err
	}

	return nil
}

func validateMaxQueueSize(value int) error {
	if value < 0 {
		return InvalidMaxQueueSize
	}
	return nil
}

func validateKeepaliveTimeout(value time.Duration) error {
	if value < 0 {
		return InvalidKeepaliveTimeout
	}
	return nil
}

func validateReconnectDelay(value time.Duration) error {
	if value < 0 {
		return InvalidReconnectDelay
	}
	return nil
}

// When KeepaliveTimeout is set to zero, keepalive timeouts are disabled.
func WithKeepaliveTimeout(value time.Duration) WorkerOption {
	return func(c *WorkerConfig) error {
		err := validateKeepaliveTimeout(value)
		if err != nil {
			return err
		}
		c.KeepaliveTimeout = value
		return nil
	}
}

func WithReconnectDelay(value time.Duration) WorkerOption {
	return func(c *WorkerConfig) error {
		err := validateReconnectDelay(value)
		if err != nil {
			return err
		}
		c.ReconnectDelay = value
		return nil
	}
}

// When MaxQueueSize is set to zero, queue limits are disabled.
func WithMaxQueueSize(value int) WorkerOption {
	return func(c *WorkerConfig) error {
		err := validateMaxQueueSize(value)
		if err != nil {
			return err
		}
		c.MaxQueueSize = value
		return nil
	}
}

func WithWorkerLoggingEnabled(value bool) WorkerOption {
	return func(c *WorkerConfig) error {
		c.LoggingEnabled = value
		return nil
	}
}

func WithWorkerLogLevel(level slog.Level) WorkerOption {
	return func(c *WorkerConfig) error {
		l := level
		c.LogLevel = &l
		return nil
	}
}
