package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewPoolConfig(t *testing.T) {
	conf, err := NewPoolConfig()
	assert.NoError(t, err)

	assert.Equal(t, conf, &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		ConnectionConfig: *transport.GetDefaultConnectionConfig(),
		WorkerConfig:     *GetDefaultWorkerConfig(),
		WorkerFactory:    nil,
	})
}

func TestDefaultPoolConfig(t *testing.T) {
	conf := GetDefaultPoolConfig()

	assert.Equal(t, conf, &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		ConnectionConfig: *transport.GetDefaultConnectionConfig(),
		WorkerConfig:     *GetDefaultWorkerConfig(),
		WorkerFactory:    nil,
	})
}

func TestApplyPoolOptions(t *testing.T) {
	conf := &PoolConfig{}
	err := applyPoolOptions(
		conf,
		WithConnectionConfig(transport.ConnectionConfig{
			Retry: transport.RetryConfig{Disabled: true},
		}),
	)

	assert.NoError(t, err)
	assert.Equal(t, 0*time.Second, conf.ConnectionConfig.WriteTimeout)
}

func TestWithBufferSizes(t *testing.T) {
	conf := &PoolConfig{}

	err := applyPoolOptions(conf,
		WithInboxBufferSize(100),
		WithEventsBufferSize(20),
	)
	assert.NoError(t, err)
	assert.Equal(t, 100, conf.InboxBufferSize)
	assert.Equal(t, 20, conf.EventsBufferSize)
}

func TestWithConnectionConfig(t *testing.T) {
	conf := &PoolConfig{}
	opt := WithConnectionConfig(transport.ConnectionConfig{
		WriteTimeout: 1 * time.Second,
		Retry:        transport.RetryConfig{Disabled: true},
	})
	err := applyPoolOptions(conf, opt)
	assert.NoError(t, err)
	assert.Equal(t, 1*time.Second, conf.ConnectionConfig.WriteTimeout)

	// invalid config is rejected
	conf = &PoolConfig{}
	opt = WithConnectionConfig(
		transport.ConnectionConfig{
			WriteTimeout: -1 * time.Second,
			Retry:        transport.RetryConfig{Disabled: true},
		})
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
			name: "valid empty (retry disabled)",
			conf: PoolConfig{
				ConnectionConfig: transport.ConnectionConfig{
					Retry: transport.RetryConfig{Disabled: true},
				},
			},
		},
		{
			name: "valid defaults",
			conf: *GetDefaultPoolConfig(),
		},
		{
			name: "valid complete",
			conf: PoolConfig{
				ConnectionConfig: transport.ConnectionConfig{
					Retry: transport.RetryConfig{Disabled: true},
				},
			},
		},
		{
			name: "invalid connection config",
			conf: PoolConfig{
				ConnectionConfig: transport.ConnectionConfig{
					Retry: transport.RetryConfig{
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
			err := ValidatePoolConfig(&tc.conf)

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
