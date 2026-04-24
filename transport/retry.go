package transport

import (
	"math"
	"math/rand"
	"time"
)

type RetryManager struct {
	config     *RetryConfig
	retryCount int
	saturation int
}

func NewRetryManager(config *RetryConfig) *RetryManager {
	// saturationCount: retry count at which base delay meets or exceeds MaxDelay.
	// Conservative by two to preserve jitter variance near the boundary.
	saturation := 0
	if config != nil &&
		config.InitialDelay > 0 &&
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
	if r.config == nil {
		return false
	}

	if r.config.MaxRetries > 0 && r.retryCount >= r.config.MaxRetries {
		return false
	}

	return true
}

func (r *RetryManager) CalculateDelay() time.Duration {
	if r.config == nil {
		return time.Second
	}

	// First attempt: immediate retry
	if r.retryCount == 0 {
		return 0
	}

	// if saturation is reached, calculated backoff will always be higher than
	// the maximum delay
	if r.config != nil && r.retryCount >= r.saturation {
		return r.config.MaxDelay
	}

	// Exponential backoff: InitialDelay * 2^(attempts-1)
	shift := r.retryCount - 1
	if shift > 62 {
		shift = 62
	} // prevent overflow
	backoffMultiplier := float64(int64(1) << shift)
	baseDelay := float64(r.config.InitialDelay) * backoffMultiplier

	// Apply jitter: delay * (1 + jitterFactor * (random - 0.5))
	random := rand.Float64()
	jitterMultiplier := 1 + r.config.JitterFactor*(random-0.5)
	delay := time.Duration(baseDelay * jitterMultiplier)

	// Cap at MaxDelay
	if delay > r.config.MaxDelay {
		delay = r.config.MaxDelay
	}

	return delay
}

func (m *RetryManager) RecordRetry() {
	m.retryCount++
}

func (m *RetryManager) RetryCount() int {
	return m.retryCount
}
