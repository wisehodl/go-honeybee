package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Config Tests

func TestNewConfig(t *testing.T) {
	conf, err := NewConnectionConfig()

	assert.NoError(t, err)
	assert.Equal(t, conf, &ConnectionConfig{
		CloseHandler: nil,
		WriteTimeout: 30 * time.Second,
		Retry:        GetDefaultRetryConfig(),
	})

	// errors propagate
	_, err = NewConnectionConfig(WithRetryMaxRetries(-1))
	assert.Error(t, err)

	_, err = NewConnectionConfig(WithRetryInitialDelay(10), WithRetryMaxDelay(1))
	assert.Error(t, err)
}

// Default Config Tests

func TestDefaultConfig(t *testing.T) {
	conf := GetDefaultConnectionConfig()

	assert.Equal(t, conf, &ConnectionConfig{
		CloseHandler: nil,
		WriteTimeout: 30 * time.Second,
		Retry:        GetDefaultRetryConfig(),
	})
}

func TestDefaultRetryConfig(t *testing.T) {
	conf := GetDefaultRetryConfig()

	assert.Equal(t, conf, &RetryConfig{
		MaxRetries:   0,
		InitialDelay: 1 * time.Second,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.5,
	})
}

// Config Builder Tests

func TestSetConfig(t *testing.T) {
	conf := &ConnectionConfig{}
	err := applyConnectionOptions(
		conf,
		WithRetryMaxRetries(0),
		WithRetryInitialDelay(3*time.Second),
		WithRetryJitterFactor(0.5),
	)

	assert.NoError(t, err)
	assert.Equal(t, 0, conf.Retry.MaxRetries)
	assert.Equal(t, 3*time.Second, conf.Retry.InitialDelay)
	assert.Equal(t, 0.5, conf.Retry.JitterFactor)

	// errors propagate
	err = applyConnectionOptions(
		conf,
		WithRetryMaxRetries(-10),
	)

	assert.ErrorIs(t, err, errors.InvalidRetryMaxRetries)
}

// Config Option Tests

func TestWithCloseHandler(t *testing.T) {
	conf := &ConnectionConfig{}
	opt := WithCloseHandler(func(code int, text string) error { return nil })
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Nil(t, conf.CloseHandler(0, ""))
}

func TestWithWriteTimeout(t *testing.T) {
	conf := &ConnectionConfig{}
	opt := WithWriteTimeout(30)
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.WriteTimeout, time.Duration(30))

	// zero allowed
	conf = &ConnectionConfig{}
	opt = WithWriteTimeout(0)
	err = applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.WriteTimeout, time.Duration(0))

	// negative disallowed
	conf = &ConnectionConfig{}
	opt = WithWriteTimeout(-30)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidWriteTimeout)
	assert.ErrorContains(t, err, "write timeout must be positive")
}

func TestWithRetry(t *testing.T) {
	conf := &ConnectionConfig{}
	opt := WithRetry()
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.NotNil(t, conf.Retry)
	assert.Equal(t, conf.Retry, GetDefaultRetryConfig())
}

func TestWithRetryAttempts(t *testing.T) {
	conf := &ConnectionConfig{}
	opt := WithRetryMaxRetries(3)
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 3, conf.Retry.MaxRetries)

	// zero allowed
	opt = WithRetryMaxRetries(0)
	err = applyConnectionOptions(conf, opt)
	assert.NoError(t, err)

	// negative disallowed
	opt = WithRetryMaxRetries(-10)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryMaxRetries)
	assert.ErrorContains(t, err, "max retry count cannot be negative")
}

func TestWithRetryInitialDelay(t *testing.T) {
	conf := &ConnectionConfig{}
	opt := WithRetryInitialDelay(10 * time.Second)
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, conf.Retry.InitialDelay)

	// zero disallowed
	opt = WithRetryInitialDelay(0 * time.Second)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryInitialDelay)
	assert.ErrorContains(t, err, "initial delay must be positive")

	// negative disallowed
	opt = WithRetryInitialDelay(-10 * time.Second)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryInitialDelay)
}

func TestWithRetryMaxDelay(t *testing.T) {
	conf := &ConnectionConfig{}
	opt := WithRetryMaxDelay(10 * time.Second)
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, conf.Retry.MaxDelay)

	// zero disallowed
	opt = WithRetryMaxDelay(0 * time.Second)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryMaxDelay)
	assert.ErrorContains(t, err, "max delay must be positive")

	// negative disallowed
	opt = WithRetryMaxDelay(-10 * time.Second)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryMaxDelay)
}

func TestWithRetryJitterFactor(t *testing.T) {
	conf := &ConnectionConfig{}

	opt := WithRetryJitterFactor(0.2)
	err := applyConnectionOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 0.2, conf.Retry.JitterFactor)

	// negative disallowed
	opt = WithRetryJitterFactor(-1)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryJitterFactor)
	assert.ErrorContains(t, err, "jitter factor must be between 0.0 and 1.0")

	// >1 disallowed
	opt = WithRetryJitterFactor(1.1)
	err = applyConnectionOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryJitterFactor)
}

// Config Validation Tests

func TestValidateConfig(t *testing.T) {
	cases := []struct {
		name        string
		conf        ConnectionConfig
		wantErr     error
		wantErrText string
	}{
		{
			name: "valid empty",
			conf: *&ConnectionConfig{},
		},
		{
			name: "valid defaults",
			conf: *GetDefaultConnectionConfig(),
		},
		{
			name: "valid complete",
			conf: ConnectionConfig{
				CloseHandler: (func(code int, text string) error { return nil }),
				WriteTimeout: time.Duration(30),
				Retry: &RetryConfig{
					MaxRetries:   0,
					InitialDelay: 2 * time.Second,
					MaxDelay:     10 * time.Second,
					JitterFactor: 0.2,
				},
			},
		},
		{
			name: "invalid - initial delay > max delay",
			conf: ConnectionConfig{
				Retry: &RetryConfig{
					InitialDelay: 10 * time.Second,
					MaxDelay:     1 * time.Second,
				},
			},
			wantErrText: "initial delay may not exceed maximum delay",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConnectionConfig(&tc.conf)

			if tc.wantErr != nil || tc.wantErrText != "" {
				if tc.wantErr != nil {
					assert.ErrorIs(t, err, tc.wantErr)
				}

				if tc.wantErrText != "" {
					assert.ErrorContains(t, err, tc.wantErrText)
				}
				return
			}

			assert.NoError(t, err)
		})
	}
}
