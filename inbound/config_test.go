// responderpool/config_test.go
package inbound

import (
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewWorkerConfig(t *testing.T) {
	conf, err := NewWorkerConfig()
	assert.NoError(t, err)
	assert.Equal(t, GetDefaultWorkerConfig(), conf)
}

func TestDefaultWorkerConfig(t *testing.T) {
	conf := GetDefaultWorkerConfig()
	assert.Equal(t, &WorkerConfig{
		MaxQueueSize:      0,
		InactivityTimeout: 0,
		LoggingEnabled:    true,
		LogLevel:          nil,
	}, conf)
}

func TestValidateWorkerConfig(t *testing.T) {
	cases := []struct {
		name    string
		conf    WorkerConfig
		wantErr error
	}{
		{
			name: "valid defaults",
			conf: *GetDefaultWorkerConfig(),
		},
		{
			name: "zero inactivity timeout disabled",
			conf: WorkerConfig{InactivityTimeout: 0},
		},
		{
			name: "positive inactivity timeout",
			conf: WorkerConfig{InactivityTimeout: 30 * time.Second},
		},
		{
			name:    "negative max queue size",
			conf:    WorkerConfig{MaxQueueSize: -1},
			wantErr: InvalidMaxQueueSize,
		},
		{
			name:    "negative inactivity timeout",
			conf:    WorkerConfig{InactivityTimeout: -1 * time.Second},
			wantErr: InvalidInactivityTimeout,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateWorkerConfig(&tc.conf)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestWithMaxQueueSize(t *testing.T) {
	conf := &WorkerConfig{}

	err := applyWorkerOptions(conf, WithMaxQueueSize(10))
	assert.NoError(t, err)
	assert.Equal(t, 10, conf.MaxQueueSize)

	err = applyWorkerOptions(conf, WithMaxQueueSize(0))
	assert.NoError(t, err)

	err = applyWorkerOptions(conf, WithMaxQueueSize(-1))
	assert.ErrorIs(t, err, InvalidMaxQueueSize)
}

func TestWithInactivityTimeout(t *testing.T) {
	conf := &WorkerConfig{}

	err := applyWorkerOptions(conf, WithInactivityTimeout(30*time.Second))
	assert.NoError(t, err)
	assert.Equal(t, 30*time.Second, conf.InactivityTimeout)

	err = applyWorkerOptions(conf, WithInactivityTimeout(0))
	assert.NoError(t, err)

	err = applyWorkerOptions(conf, WithInactivityTimeout(-1*time.Second))
	assert.ErrorIs(t, err, InvalidInactivityTimeout)
}

func TestNewPoolConfig(t *testing.T) {
	conf, err := NewPoolConfig()
	assert.NoError(t, err)
	assert.Equal(t, GetDefaultPoolConfig(), conf)
}

func TestDefaultPoolConfig(t *testing.T) {
	conf := GetDefaultPoolConfig()
	assert.Equal(t, &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		LoggingEnabled:   true,
		LogLevel:         nil,
		ConnectionConfig: nil,
		WorkerConfig:     nil,
		WorkerFactory:    nil,
	}, conf)
}

func TestValidatePoolConfig(t *testing.T) {
	cases := []struct {
		name        string
		conf        PoolConfig
		wantErrText string
	}{
		{
			name: "valid empty",
			conf: PoolConfig{},
		},
		{
			name: "valid defaults",
			conf: *GetDefaultPoolConfig(),
		},
		{
			name: "valid with configs",
			conf: PoolConfig{
				ConnectionConfig: &transport.ConnectionConfig{},
				WorkerConfig:     &WorkerConfig{},
			},
		},
		{
			name: "invalid connection config",
			conf: PoolConfig{
				ConnectionConfig: &transport.ConnectionConfig{
					Retry: &transport.RetryConfig{
						InitialDelay: 10 * time.Second,
						MaxDelay:     1 * time.Second,
					},
				},
			},
			wantErrText: "initial delay may not exceed maximum delay",
		},
		{
			name: "invalid worker config",
			conf: PoolConfig{
				WorkerConfig: &WorkerConfig{MaxQueueSize: -1},
			},
			wantErrText: "maximum queue size cannot be negative",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePoolConfig(&tc.conf)
			if tc.wantErrText != "" {
				assert.ErrorContains(t, err, tc.wantErrText)
				return
			}
			assert.NoError(t, err)
		})
	}
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

	err := applyPoolOptions(conf, WithConnectionConfig(&transport.ConnectionConfig{}))
	assert.NoError(t, err)
	assert.NotNil(t, conf.ConnectionConfig)

	err = applyPoolOptions(conf, WithConnectionConfig(&transport.ConnectionConfig{
		Retry: &transport.RetryConfig{
			InitialDelay: 10 * time.Second,
			MaxDelay:     1 * time.Second,
		},
	}))
	assert.Error(t, err)
}

func TestWithWorkerConfig(t *testing.T) {
	conf := &PoolConfig{}

	err := applyPoolOptions(conf, WithWorkerConfig(&WorkerConfig{InactivityTimeout: 30 * time.Second}))
	assert.NoError(t, err)
	assert.Equal(t, 30*time.Second, conf.WorkerConfig.InactivityTimeout)

	err = applyPoolOptions(conf, WithWorkerConfig(&WorkerConfig{MaxQueueSize: -1}))
	assert.Error(t, err)
}
