// responderpool/config.go
package inbound

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"time"
)

// Worker Config

type WorkerConfig struct {
	MaxQueueSize      int
	InactivityTimeout time.Duration
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
		MaxQueueSize:      0, // queue can grow indefinitely by default
		InactivityTimeout: 0, // eviction disabled by default
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
	if err := validateMaxQueueSize(config.MaxQueueSize); err != nil {
		return err
	}
	if err := validateDeadTimeout(config.InactivityTimeout); err != nil {
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

func validateDeadTimeout(value time.Duration) error {
	if value < 0 {
		return InvalidDeadTimeout
	}
	return nil
}

// When MaxQueueSize is zero, queue limits are disabled.
func WithMaxQueueSize(value int) WorkerOption {
	return func(c *WorkerConfig) error {
		if err := validateMaxQueueSize(value); err != nil {
			return err
		}
		c.MaxQueueSize = value
		return nil
	}
}

// When DeadTimeout is zero, the watchdog is disabled.
func WithDeadTimeout(value time.Duration) WorkerOption {
	return func(c *WorkerConfig) error {
		if err := validateDeadTimeout(value); err != nil {
			return err
		}
		c.InactivityTimeout = value
		return nil
	}
}

// Pool Config

type WorkerFactory func(
	ctx context.Context,
	id string,
	conn *transport.Connection,
	config *WorkerConfig,
) (Worker, error)

type PoolConfig struct {
	ConnectionConfig *transport.ConnectionConfig
	WorkerConfig     *WorkerConfig
	WorkerFactory    WorkerFactory
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
		WorkerConfig:     nil,
		WorkerFactory:    nil,
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
	if config.ConnectionConfig != nil {
		if err := transport.ValidateConnectionConfig(config.ConnectionConfig); err != nil {
			return err
		}
	}
	if config.WorkerConfig != nil {
		if err := ValidateWorkerConfig(config.WorkerConfig); err != nil {
			return err
		}
	}
	return nil
}

func WithConnectionConfig(cc *transport.ConnectionConfig) PoolOption {
	return func(c *PoolConfig) error {
		if err := transport.ValidateConnectionConfig(cc); err != nil {
			return err
		}
		c.ConnectionConfig = cc
		return nil
	}
}

func WithWorkerConfig(wc *WorkerConfig) PoolOption {
	return func(c *PoolConfig) error {
		if err := ValidateWorkerConfig(wc); err != nil {
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
