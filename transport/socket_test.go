package transport

import (
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync/atomic"
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
			attemptIndex := atomic.Int32{}
			mockDialer := &honeybeetest.MockDialer{
				DialContextFunc: func(context.Context, string, http.Header,
				) (types.Socket, *http.Response, error) {
					err := tc.mockRuns[attemptIndex.Load()]
					attemptIndex.Add(1)
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
				context.Background(), retryMgr, mockDialer, "ws://test", nil, nil)

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
				context.Background(), tc.retryMgr, tc.dialer, tc.url, nil, nil)

			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.Nil(t, socket)
			assert.Nil(t, resp)
		})
	}
}

func TestAcquireSocketContextCancellation(t *testing.T) {
	t.Run("already-canceled context returns immediately without dialing",
		func(t *testing.T) {
			dialCalled := atomic.Bool{}
			mockDialer := &honeybeetest.MockDialer{
				DialContextFunc: func(ctx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
					dialCalled.Store(true)
					return honeybeetest.NewMockSocket(), nil, nil
				},
			}

			ctx, cancel := context.WithCancel(context.Background())

			// cancel before acquiring socket
			cancel()

			retryMgr := NewRetryManager(GetDefaultRetryConfig())
			_, _, err := AcquireSocket(ctx, retryMgr, mockDialer, "ws://test", nil, nil)

			assert.ErrorIs(t, err, context.Canceled)
			assert.False(t, dialCalled.Load())
		})

	t.Run("context cancelled during sleep returns before next attempt",
		func(t *testing.T) {
			dialCount := atomic.Int32{}
			mockDialer := &honeybeetest.MockDialer{
				DialContextFunc: func(_ context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
					dialCount.Add(1)
					return nil, nil, fmt.Errorf("dial failed")
				},
			}

			ctx, cancel := context.WithCancel(context.Background())

			retryMgr := NewRetryManager(&RetryConfig{
				MaxRetries:   10,
				InitialDelay: 1 * time.Second,
				MaxDelay:     1 * time.Second,
				JitterFactor: 0.0,
			})

			done := make(chan error, 1)
			go func() {
				_, _, err := AcquireSocket(ctx, retryMgr, mockDialer, "ws://test", nil, nil)
				done <- err
			}()

			// wait for first two dials to complete, then cancel during sleep
			honeybeetest.Eventually(t, func() bool {
				return dialCount.Load() > 1
			}, "expected dials")
			cancel()

			select {
			case err := <-done:
				assert.ErrorIs(t, err, context.Canceled)

				// dial count is 2 because the first retry is always immediate
				assert.Equal(t, int32(2), dialCount.Load())
			case <-time.After(honeybeetest.TestTimeout):
				t.Fatal("AcquireSocket did not return after context cancellation")
			}
		})

	t.Run("context cancelled during in-progress dial unblocks and returns",
		func(t *testing.T) {
			mockDialer := &honeybeetest.MockDialer{
				DialContextFunc: func(ctx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
					// block until context is cancelled
					select {
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					}
				},
			}

			ctx, cancel := context.WithCancel(context.Background())

			retryMgr := NewRetryManager(GetDefaultRetryConfig())
			done := make(chan error, 1)
			go func() {
				_, _, err := AcquireSocket(ctx, retryMgr, mockDialer, "ws://test", nil, nil)
				done <- err
			}()

			// wait for dialer to block
			time.Sleep(20 * time.Millisecond)
			cancel()

			select {
			case err := <-done:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(honeybeetest.TestTimeout):
				t.Fatal("AcquireSocket did not return after context cancellation")
			}

		})
}

func TestAcquireSocketPassesHeaders(t *testing.T) {
	header := http.Header{"User-Agent": []string{"test-agent"}}
	dialCalled := false

	mockDialer := &honeybeetest.MockDialer{
		DialContextFunc: func(ctx context.Context, url string, h http.Header) (types.Socket, *http.Response, error) {
			assert.Equal(t, "test-agent", h.Get("User-Agent"))
			dialCalled = true
			return honeybeetest.NewMockSocket(), nil, nil
		},
	}

	retryMgr := NewRetryManager(&RetryConfig{MaxRetries: 0})
	_, _, err := AcquireSocket(context.Background(), retryMgr, mockDialer, "ws://test", header, nil)

	assert.NoError(t, err)
	assert.True(t, dialCalled)
}
