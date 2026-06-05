package transport

import (
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
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
	assert.NotNil(t, dialer.Dialer.NetDialContext)
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

			retryMgr := NewRetryManager(RetryConfig{
				MaxRetries:   tc.maxRetries,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			})

			socket, _, err := AcquireSocket(
				context.Background(), retryMgr, mockDialer, "ws://test", nil, nil, nil)

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
	validRetryConfig := GetDefaultConnectionConfig().Retry
	validRetryMgr := NewRetryManager(validRetryConfig)

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
				context.Background(), tc.retryMgr, tc.dialer, tc.url, nil, nil, nil)

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

			retryMgr := NewRetryManager(GetDefaultConnectionConfig().Retry)
			_, _, err := AcquireSocket(ctx, retryMgr, mockDialer, "ws://test", nil, nil, nil)

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

			retryMgr := NewRetryManager(RetryConfig{
				MaxRetries:   10,
				InitialDelay: 1 * time.Second,
				MaxDelay:     1 * time.Second,
				JitterFactor: 0.0,
			})

			done := make(chan error, 1)
			go func() {
				_, _, err := AcquireSocket(ctx, retryMgr, mockDialer, "ws://test", nil, nil, nil)
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

			retryMgr := NewRetryManager(GetDefaultConnectionConfig().Retry)
			done := make(chan error, 1)
			go func() {
				_, _, err := AcquireSocket(ctx, retryMgr, mockDialer, "ws://test", nil, nil, nil)
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

func TestAcquireSocketOnDialError(t *testing.T) {
	t.Run("callback fires once per failed attempt with exact error", func(t *testing.T) {
		var mu sync.Mutex
		var capturedErrors []error

		dialErr1 := errors.New("attempt 1 failed")
		dialErr2 := errors.New("attempt 2 failed")
		attemptIndex := atomic.Int32{}
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				i := attemptIndex.Add(1)
				switch i {
				case 1:
					return nil, nil, dialErr1
				case 2:
					return nil, nil, dialErr2
				default:
					return honeybeetest.NewMockSocket(), nil, nil
				}
			},
		}

		onDialError := func(err error) {
			mu.Lock()
			defer mu.Unlock()
			capturedErrors = append(capturedErrors, err)
		}

		retryMgr := NewRetryManager(RetryConfig{
			MaxRetries:   3,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			JitterFactor: 0.0,
		})
		_, _, err := AcquireSocket(context.Background(), retryMgr, mockDialer, "ws://test", nil, nil, onDialError)

		assert.NoError(t, err)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []error{dialErr1, dialErr2}, capturedErrors)
	})

	t.Run("callback not called on successful first dial", func(t *testing.T) {
		callbackCalled := atomic.Bool{}
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}

		retryMgr := NewRetryManager(RetryConfig{MaxRetries: 3, InitialDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond})
		_, _, err := AcquireSocket(context.Background(), retryMgr, mockDialer, "ws://test", nil, nil, func(error) {
			callbackCalled.Store(true)
		})

		assert.NoError(t, err)
		assert.False(t, callbackCalled.Load())
	})

	t.Run("nil callback does not panic on failed dial", func(t *testing.T) {
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, errors.New("dial failed")
			},
		}

		retryMgr := NewRetryManager(RetryConfig{Disabled: true})
		assert.NotPanics(t, func() {
			AcquireSocket(context.Background(), retryMgr, mockDialer, "ws://test", nil, nil, nil)
		})
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

	retryMgr := NewRetryManager(RetryConfig{MaxRetries: 0, InitialDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond})
	_, _, err := AcquireSocket(context.Background(), retryMgr, mockDialer, "ws://test", header, nil, nil)

	assert.NoError(t, err)
	assert.True(t, dialCalled)
}
