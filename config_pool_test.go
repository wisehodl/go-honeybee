package honeybee

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewPoolConfig(t *testing.T) {
	conf, err := NewInitiatorPoolConfig()
	assert.NoError(t, err)

	assert.Equal(t, conf, &InitiatorPoolConfig{
		ConnectionConfig: nil,
		WorkerConfig:     nil,
		WorkerFactory:    nil,
	})
}

func TestDefaultPoolConfig(t *testing.T) {
	conf := GetDefaultInitiatorPoolConfig()

	assert.Equal(t, conf, &InitiatorPoolConfig{
		ConnectionConfig: nil,
		WorkerConfig:     nil,
		WorkerFactory:    nil,
	})
}

func TestApplyPoolOptions(t *testing.T) {
	conf := &InitiatorPoolConfig{}
	err := applyInitiatorPoolOptions(
		conf,
		WithInitiatorConnectionConfig(&ConnectionConfig{}),
	)

	assert.NoError(t, err)
	assert.Equal(t, 0*time.Second, conf.ConnectionConfig.WriteTimeout)
}

func TestWithConnectionConfig(t *testing.T) {
	conf := &InitiatorPoolConfig{}
	opt := WithInitiatorConnectionConfig(&ConnectionConfig{WriteTimeout: 1 * time.Second})
	err := applyInitiatorPoolOptions(conf, opt)
	assert.NoError(t, err)
	assert.NotNil(t, conf.ConnectionConfig)
	assert.Equal(t, 1*time.Second, conf.ConnectionConfig.WriteTimeout)

	// invalid config is rejected
	conf = &InitiatorPoolConfig{}
	opt = WithInitiatorConnectionConfig(&ConnectionConfig{WriteTimeout: -1 * time.Second})
	err = applyInitiatorPoolOptions(conf, opt)
	assert.Error(t, err)
}

func TestValidatePoolConfig(t *testing.T) {
	cases := []struct {
		name        string
		conf        InitiatorPoolConfig
		wantErr     error
		wantErrText string
	}{
		{
			name: "valid empty",
			conf: *&InitiatorPoolConfig{},
		},
		{
			name: "valid defaults",
			conf: *GetDefaultInitiatorPoolConfig(),
		},
		{
			name: "valid complete",
			conf: InitiatorPoolConfig{
				ConnectionConfig: &ConnectionConfig{},
			},
		},
		{
			name: "invalid connection config",
			conf: InitiatorPoolConfig{
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
			err := validateInitiatorPoolConfig(&tc.conf)

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
