package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewPoolConfig(t *testing.T) {
	conf, err := NewPoolConfig()
	assert.NoError(t, err)

	assert.Equal(t, conf, &PoolConfig{
		IdleTimeout:      20 * time.Second,
		ConnectionConfig: nil,
	})

	// errors propagate
	_, err = NewPoolConfig(WithIdleTimeout(-1))
	assert.Error(t, err)
}

func TestDefaultPoolConfig(t *testing.T) {
	conf := GetDefaultPoolConfig()

	assert.Equal(t, conf, &PoolConfig{
		IdleTimeout:      20 * time.Second,
		ConnectionConfig: nil,
	})
}

func TestApplyPoolOptions(t *testing.T) {
	conf := &PoolConfig{}
	err := applyPoolOptions(
		conf,
		WithIdleTimeout(15),
		WithConnectionConfig(&ConnectionConfig{}),
	)

	assert.NoError(t, err)
	assert.Equal(t, time.Duration(15), conf.IdleTimeout)
	assert.Equal(t, 0*time.Second, conf.ConnectionConfig.WriteTimeout)

	// errors propagate
	err = applyPoolOptions(
		conf,
		WithIdleTimeout(-1),
	)

	assert.ErrorIs(t, err, errors.InvalidIdleTimeout)
}

func TestWithIdleTimeout(t *testing.T) {
	conf := &PoolConfig{}
	opt := WithIdleTimeout(30)
	err := applyPoolOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.IdleTimeout, time.Duration(30))

	// zero allowed
	conf = &PoolConfig{}
	opt = WithIdleTimeout(0)
	err = applyPoolOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, conf.IdleTimeout, time.Duration(0))

	// negative disallowed
	conf = &PoolConfig{}
	opt = WithIdleTimeout(-30)
	err = applyPoolOptions(conf, opt)
	assert.ErrorIs(t, err, errors.InvalidIdleTimeout)
	assert.ErrorContains(t, err, "idle timeout cannot be negative")
}

func TestWithConnectionConfig(t *testing.T) {
	conf := &PoolConfig{}
	opt := WithConnectionConfig(&ConnectionConfig{WriteTimeout: 1 * time.Second})
	err := applyPoolOptions(conf, opt)
	assert.NoError(t, err)
	assert.NotNil(t, conf.ConnectionConfig)
	assert.Equal(t, 1*time.Second, conf.ConnectionConfig.WriteTimeout)

	// invalid config is rejected
	conf = &PoolConfig{}
	opt = WithConnectionConfig(&ConnectionConfig{WriteTimeout: -1 * time.Second})
	err = applyPoolOptions(conf, opt)
	assert.Error(t, err)
}

func TestValidatePoolConfig(t *testing.T) {
	cases := []struct {
		name        string
		conf        PoolConfig
		wantErr     error
		wantErrText string
	}{
		{
			name: "valid empty",
			conf: *&PoolConfig{},
		},
		{
			name: "valid defaults",
			conf: *GetDefaultPoolConfig(),
		},
		{
			name: "valid complete",
			conf: PoolConfig{
				IdleTimeout:      15 * time.Second,
				ConnectionConfig: &ConnectionConfig{},
			},
		},
		{
			name: "invalid connection config",
			conf: PoolConfig{
				ConnectionConfig: &ConnectionConfig{
					Retry: &RetryConfig{
						InitialDelay: 10 * time.Second,
						MaxDelay:     1 * time.Second,
					},
				},
			},
			wantErrText: "initial delay may not exceed maximum delay",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePoolConfig(&tc.conf)

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
