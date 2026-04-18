package transport

import (
	"context"
	"errors"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestNewDialer(t *testing.T) {
	dialer := NewDialer()

	assert.NotNil(t, dialer)
	_, ok := dialer.(*GorillaDialer)
	assert.True(t, ok, "NewDialer should return *GorillaDialer")
}

func TestNewGorillaDialer(t *testing.T) {
	dialer := NewGorillaDialer()

	assert.NotNil(t, dialer)
	assert.NotNil(t, dialer.Dialer)
	assert.Equal(t, 45*time.Second, dialer.Dialer.HandshakeTimeout)
	assert.Equal(t, 1024, dialer.Dialer.ReadBufferSize)
	assert.Equal(t, 1024, dialer.Dialer.WriteBufferSize)
}

func TestAcquireSocket(t *testing.T) {
	cases := []struct {
		name           string
		mockRuns       []error
		maxRetries     int
		wantRetryCount int
		wantErr        bool
	}{
		{
			name:           "immediate success",
			mockRuns:       []error{nil},
			maxRetries:     3,
			wantRetryCount: 0,
			wantErr:        false,
		},
		{
			name:           "two failures, success",
			mockRuns:       []error{errors.New("1"), errors.New("2"), nil},
			maxRetries:     0,
			wantRetryCount: 2,
			wantErr:        false,
		},
		{
			name:           "three failures, failure",
			mockRuns:       []error{errors.New("1"), errors.New("2"), errors.New("3"), errors.New("4")},
			maxRetries:     3,
			wantRetryCount: 3,
			wantErr:        true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attemptIndex := 0
			mockDialer := &honeybeetest.MockDialer{
				DialContextFunc: func(context.Context, string, http.Header,
				) (types.Socket, *http.Response, error) {
					err := tc.mockRuns[attemptIndex]
					attemptIndex++
					if err != nil {
						return nil, nil, err
					}
					return honeybeetest.NewMockSocket(), nil, nil
				},
			}

			retryMgr := NewRetryManager(&RetryConfig{
				MaxRetries:   tc.maxRetries,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			})

			socket, _, err := AcquireSocket(
				context.Background(), retryMgr, mockDialer, "ws://test", nil)

			assert.Equal(t, tc.wantRetryCount, retryMgr.RetryCount())
			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, socket)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, socket)
			}
		})
	}
}

func TestAcquireSocketGuards(t *testing.T) {
	validDialer := &honeybeetest.MockDialer{
		DialContextFunc: func(context.Context, string, http.Header,
		) (types.Socket, *http.Response, error) {
			return honeybeetest.NewMockSocket(), nil, nil
		},
	}
	validRetryMgr := NewRetryManager(GetDefaultRetryConfig())

	cases := []struct {
		name     string
		retryMgr *RetryManager
		dialer   types.Dialer
		url      string
		wantErr  string
	}{
		{
			name:     "nil retry manager",
			retryMgr: nil,
			dialer:   validDialer,
			url:      "ws://test",
			wantErr:  "retry manager cannot be nil",
		},
		{
			name:     "nil dialer",
			retryMgr: validRetryMgr,
			dialer:   nil,
			url:      "ws://test",
			wantErr:  "dialer cannot be nil",
		},
		{
			name:     "empty URL",
			retryMgr: validRetryMgr,
			dialer:   validDialer,
			url:      "",
			wantErr:  "URL cannot be empty",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			socket, resp, err := AcquireSocket(
				context.Background(), tc.retryMgr, tc.dialer, tc.url, nil)

			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.Nil(t, socket)
			assert.Nil(t, resp)
		})
	}
}
