package transport

import (
	"math"
	"math/rand"
	"time"
)

type RetryManager struct {
	config     *RetryConfig
	retryCount int
}

func NewRetryManager(config *RetryConfig) *RetryManager {
	return &RetryManager{
		config:     config,
		retryCount: 0,
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

	// Exponential backoff: InitialDelay * 2^(attempts-1)
	backoffMultiplier := math.Pow(2, float64(r.retryCount-1))
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
