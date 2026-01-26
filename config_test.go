package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Config Tests

func TestNewConfig(t *testing.T) {
	conf, err := NewConfig(WithRetry())

	assert.NoError(t, err)
	assert.Equal(t, conf, &Config{
		Retry: GetDefaultRetryConfig(),
	})

	// errors propagate
	_, err = NewConfig(WithRetryMaxRetries(-1))
	assert.Error(t, err)

	_, err = NewConfig(WithRetryInitialDelay(10), WithRetryMaxDelay(1))
	assert.Error(t, err)
}

// Default Config Tests

func TestDefaultConfig(t *testing.T) {
	conf := GetDefaultConfig()

	assert.Nil(t, conf.CloseHandler)
	assert.Nil(t, conf.Retry)
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
	conf := GetDefaultConfig()
	err := SetConfig(
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
	err = SetConfig(
		conf,
		WithRetryMaxRetries(-10),
	)

	assert.ErrorIs(t, err, errors.InvalidRetryMaxRetries)
}

// Config Option Tests

func TestWithCloseHandler(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithCloseHandler(func(code int, text string) error { return nil })
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Nil(t, conf.CloseHandler(0, ""))
}

func TestWithReadTimeout(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithReadTimeout(30)
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.ReadTimeout, time.Duration(30))

	// zero allowed
	conf = GetDefaultConfig()
	opt = WithReadTimeout(0)
	err = SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.ReadTimeout, time.Duration(0))

	// negative disallowed
	conf = GetDefaultConfig()
	opt = WithReadTimeout(-30)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidReadTimeout)
	assert.ErrorContains(t, err, "read timeout must be positive")
}

func TestWithWriteTimeout(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithWriteTimeout(30)
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.WriteTimeout, time.Duration(30))

	// zero allowed
	conf = GetDefaultConfig()
	opt = WithWriteTimeout(0)
	err = SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.WriteTimeout, time.Duration(0))

	// negative disallowed
	conf = GetDefaultConfig()
	opt = WithWriteTimeout(-30)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidWriteTimeout)
	assert.ErrorContains(t, err, "write timeout must be positive")
}

func TestWithRetry(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithRetry()
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.NotNil(t, conf.Retry)
	assert.Equal(t, conf.Retry, GetDefaultRetryConfig())
}

func TestWithRetryAttempts(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithRetryMaxRetries(3)
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 3, conf.Retry.MaxRetries)

	// zero allowed
	opt = WithRetryMaxRetries(0)
	err = SetConfig(conf, opt)
	assert.NoError(t, err)

	// negative disallowed
	opt = WithRetryMaxRetries(-10)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryMaxRetries)
	assert.ErrorContains(t, err, "max retry count cannot be negative")
}

func TestWithRetryInitialDelay(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithRetryInitialDelay(10 * time.Second)
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, conf.Retry.InitialDelay)

	// zero disallowed
	opt = WithRetryInitialDelay(0 * time.Second)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryInitialDelay)
	assert.ErrorContains(t, err, "initial delay must be positive")

	// negative disallowed
	opt = WithRetryInitialDelay(-10 * time.Second)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryInitialDelay)
}

func TestWithRetryMaxDelay(t *testing.T) {
	conf := GetDefaultConfig()
	opt := WithRetryMaxDelay(10 * time.Second)
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, conf.Retry.MaxDelay)

	// zero disallowed
	opt = WithRetryMaxDelay(0 * time.Second)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryMaxDelay)
	assert.ErrorContains(t, err, "max delay must be positive")

	// negative disallowed
	opt = WithRetryMaxDelay(-10 * time.Second)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryMaxDelay)
}

func TestWithRetryJitterFactor(t *testing.T) {
	conf := GetDefaultConfig()

	opt := WithRetryJitterFactor(0.2)
	err := SetConfig(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 0.2, conf.Retry.JitterFactor)

	// negative disallowed
	opt = WithRetryJitterFactor(-1)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryJitterFactor)
	assert.ErrorContains(t, err, "jitter factor must be between 0.0 and 1.0")

	// >1 disallowed
	opt = WithRetryJitterFactor(1.1)
	err = SetConfig(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidRetryJitterFactor)
}

// Config Validation Tests

func TestValidateConfig(t *testing.T) {
	cases := []struct {
		name        string
		conf        Config
		wantErr     error
		wantErrText string
	}{
		{
			name: "valid empty",
			conf: *GetDefaultConfig(),
		},
		{
			name: "valid defaults",
			conf: *GetDefaultConfig(),
		},
		{
			name: "valid complete",
			conf: Config{
				CloseHandler: (func(code int, text string) error { return nil }),
				ReadTimeout:  time.Duration(30),
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
			conf: Config{
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
			err := ValidateConfig(&tc.conf)

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
