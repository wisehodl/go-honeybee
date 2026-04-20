package outbound

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"time"
)

// Types

type WorkerFactory func(ctx context.Context, id string) (Worker, error)

// Pool Config

type PoolConfig struct {
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
	MaxQueueSize     int
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
		KeepaliveTimeout: 20 * time.Second,
		MaxQueueSize:     0, // disabled by default
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
