package transport

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type RetryConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	JitterFactor float64
}

func NewRetryConfig(opts ...RetryOption) (RetryConfig, error) {
	conf := RetryConfig{
		MaxRetries:   0, // Infinite retries
		InitialDelay: 1 * time.Second,
		MaxDelay:     60 * time.Second,
		JitterFactor: 0.2,
	}
	for _, o := range opts {
		o(&conf)
	}

	err := ValidateRetryConfig(conf)
	if err != nil {
		return RetryConfig{}, err
	}

	return conf, nil
}

type RetryOption func(*RetryConfig)

func WithMaxRetries(value int) RetryOption {
	return func(c *RetryConfig) {
		c.MaxRetries = value
	}
}

func WithInitialDelay(value time.Duration) RetryOption {
	return func(c *RetryConfig) {
		c.InitialDelay = value
	}
}

func WithMaxDelay(value time.Duration) RetryOption {
	return func(c *RetryConfig) {
		c.MaxDelay = value
	}
}

func WithJitterFactor(value float64) RetryOption {
	return func(c *RetryConfig) {
		c.JitterFactor = value
	}
}

func ValidateRetryConfig(c RetryConfig) error {
	if c.MaxRetries < 0 {
		return fmt.Errorf("invalid max retry count: %d", c.MaxRetries)
	}

	if c.InitialDelay <= 0 {
		return fmt.Errorf("invalid initial delay: %v", c.InitialDelay)
	}

	if c.MaxDelay <= 0 {
		return fmt.Errorf("invalid max delay: %v", c.MaxDelay)
	}

	if c.JitterFactor < 0.0 || c.JitterFactor > 1.0 {
		return fmt.Errorf("invalid jitter factor: %f", c.JitterFactor)
	}

	if c.InitialDelay > c.MaxDelay {
		return fmt.Errorf("Initial delay %v cannot exceed max delay %v",
			c.InitialDelay, c.MaxDelay)
	}

	return nil
}

// ----------------------------------------------------------------------------
// Retry manager
// ----------------------------------------------------------------------------

type RetryManager struct {
	config     RetryConfig
	retryCount int
	saturation int
}

func NewRetryManager(config RetryConfig) *RetryManager {
	// saturationCount: retry count at which base delay meets or exceeds MaxDelay.
	// Conservative by two to preserve jitter variance near the boundary.
	saturation := 0
	if config.InitialDelay > 0 &&
		config.InitialDelay <= config.MaxDelay {
		ratio := float64(config.MaxDelay) / float64(config.InitialDelay)
		saturation = int(math.Ceil(math.Log2(ratio))) + 2
	}

	return &RetryManager{
		config:     config,
		retryCount: 0,
		saturation: saturation,
	}
}

func (r *RetryManager) ShouldRetry() bool {
	if r.config.MaxRetries > 0 && r.retryCount >= r.config.MaxRetries {
		return false
	}

	return true
}

func (r *RetryManager) CalculateDelay() time.Duration {
	// First attempt: immediate retry
	if r.retryCount == 0 {
		return 0
	}

	// if saturation is reached, calculated backoff will always be higher than
	// the maximum delay
	if r.retryCount >= r.saturation {
		return r.config.MaxDelay
	}

	// Exponential backoff: InitialDelay * 2^(attempts-1)
	shift := min(r.retryCount-1, 62) // prevent overflow
	backoffMultiplier := float64(int64(1) << shift)
	baseDelay := float64(r.config.InitialDelay) * backoffMultiplier

	// Apply jitter: delay * (1 + jitterFactor * (random - 0.5))
	random := rand.Float64()
	jitterMultiplier := 1 + r.config.JitterFactor*(random-0.5)
	delay := min(
		// Cap at MaxDelay
		time.Duration(baseDelay*jitterMultiplier), r.config.MaxDelay)

	return delay
}

func (m *RetryManager) RecordRetry() {
	m.retryCount++
}

func (m *RetryManager) RetryCount() int {
	return m.retryCount
}
