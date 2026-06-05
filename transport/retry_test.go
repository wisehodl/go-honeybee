package transport

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewRetryManager(t *testing.T) {
	config, _ := NewRetryConfig(WithMaxRetries(0))

	mgr := NewRetryManager(config)

	assert.Equal(t, config, mgr.config)
	assert.Equal(t, 0, mgr.retryCount)
}

func TestRecordRetry(t *testing.T) {
	config, _ := NewRetryConfig(WithMaxRetries(0))
	mgr := NewRetryManager(config)
	assert.Equal(t, mgr.retryCount, 0)

	mgr.RecordRetry()
	assert.Equal(t, mgr.retryCount, 1)

	mgr.RecordRetry()
	assert.Equal(t, mgr.retryCount, 2)
}

func TestShouldRetry(t *testing.T) {
	// always retry if max attempt count is zero
	config, _ := NewRetryConfig(WithMaxRetries(0))
	mgr := &RetryManager{
		config:     config,
		retryCount: 1000,
	}
	assert.True(t, mgr.ShouldRetry())

	// retry if below max attempt count
	config, _ = NewRetryConfig(WithMaxRetries(10))
	mgr = &RetryManager{
		config:     config,
		retryCount: 5,
	}
	assert.True(t, mgr.ShouldRetry())

	// do not retry if above max attempt count
	config, _ = NewRetryConfig(WithMaxRetries(10))
	mgr = &RetryManager{
		config:     config,
		retryCount: 11,
	}
	assert.False(t, mgr.ShouldRetry())
}

func TestCalculateDelayWithoutJitter(t *testing.T) {
	mgr := NewRetryManager(RetryConfig{
		MaxRetries:   0,
		InitialDelay: 1 * time.Second,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.0,
	})

	// Retry 0: immediate
	assert.Equal(t, 0*time.Second, mgr.CalculateDelay())
	mgr.RecordRetry()

	// Retry 1: 1s * 2^0 = 1s
	assert.Equal(t, 1*time.Second, mgr.CalculateDelay())
	mgr.RecordRetry()

	// Retry 2: 1s * 2^1 = 2s
	assert.Equal(t, 2*time.Second, mgr.CalculateDelay())
	mgr.RecordRetry()

	// Retry 3: 1s * 2^2 = 4s
	assert.Equal(t, 4*time.Second, mgr.CalculateDelay())
	mgr.RecordRetry()

	// Retry 4: 1s * 2^3 = 8s, capped at 5s
	assert.Equal(t, 5*time.Second, mgr.CalculateDelay())
	mgr.RecordRetry()

	// Retry 5: Still capped at 5s
	assert.Equal(t, 5*time.Second, mgr.CalculateDelay())
}

func TestCalculateDelayWithJitter(t *testing.T) {
	mgr := NewRetryManager(RetryConfig{
		MaxRetries:   0,
		InitialDelay: 1 * time.Second,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.5,
	})

	// Retry 0: immediate
	assert.Equal(t, 0*time.Second, mgr.CalculateDelay())
	mgr.RecordRetry()

	// Retry 1: 1s * 2^0 = 1s (with jitter)
	delay := mgr.CalculateDelay()
	assert.GreaterOrEqual(t, delay, 750*time.Millisecond)
	assert.LessOrEqual(t, delay, 1250*time.Millisecond)
	mgr.RecordRetry()

	// Retry 2: 1s * 2^1 = 2s (with jitter)
	delay = mgr.CalculateDelay()
	assert.GreaterOrEqual(t, delay, 1500*time.Millisecond)
	assert.LessOrEqual(t, delay, 2500*time.Millisecond)
	mgr.RecordRetry()

	// Retry 3: 1s * 2^2 = 4s (with jitter)
	delay = mgr.CalculateDelay()
	assert.GreaterOrEqual(t, delay, 3*time.Second)
	assert.LessOrEqual(t, delay, 5*time.Second)
	mgr.RecordRetry()

	// Retry 4: 1s * 2^3 = 8s, capped at 5s (with jitter)
	delay = mgr.CalculateDelay()
	assert.GreaterOrEqual(t, delay, 3750*time.Millisecond)
	assert.LessOrEqual(t, delay, 5*time.Second)
	mgr.RecordRetry()

	// Retry 5: Still capped at 5s (with jitter)
	delay = mgr.CalculateDelay()
	assert.GreaterOrEqual(t, delay, 3750*time.Millisecond)
	assert.LessOrEqual(t, delay, 5*time.Second)
}
