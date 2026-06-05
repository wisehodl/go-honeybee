package transport

import (
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
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
			name: "two failures, success",
			mockRuns: []error{
				errors.New("1"),
				errors.New("2"),
				nil},
			maxRetries:     0,
			wantRetryCount: 2,
			wantErr:        false,
		},
		{
			name: "three failures, failure",
			mockRuns: []error{
				errors.New("1"),
				errors.New("2"),
				errors.New("3"),
				errors.New("4")},
			maxRetries:     3,
			wantRetryCount: 3,
			wantErr:        true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attemptIndex := atomic.Int32{}
			dial := func(ctx context.Context) (types.Socket, error) {
				err := tc.mockRuns[attemptIndex.Load()]
				attemptIndex.Add(1)
				if err != nil {
					return nil, err
				}
				return honeybeetest.NewMockSocket(), nil
			}

			retryMgr := NewRetryManager(RetryConfig{
				MaxRetries:   tc.maxRetries,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			})

			socket, err := AcquireSocket(
				context.Background(), retryMgr, dial, nil, nil)

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

	t.Run("no retry, no errors channel", func(t *testing.T) {
		dial := func(ctx context.Context) (types.Socket, error) {
			return nil, errors.New("dial failed")
		}
		_, err := AcquireSocket(context.Background(), nil, dial, nil, nil)
		assert.Error(t, err)
	})
}

func TestAcquireSocketNoRetry(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dial := func(ctx context.Context) (types.Socket, error) {
			return honeybeetest.NewMockSocket(), nil
		}
		socket, err := AcquireSocket(context.Background(), nil, dial, nil, nil)
		assert.NotNil(t, socket)
		assert.NoError(t, err)
	})

	t.Run("error", func(t *testing.T) {
		dial := func(ctx context.Context) (types.Socket, error) {
			return nil, errors.New("dial failed")
		}
		_, err := AcquireSocket(context.Background(), nil, dial, nil, nil)
		assert.Error(t, err)
	})
}

func TestAcquireSocketGuards(t *testing.T) {
	validRetryConfig, _ := NewRetryConfig()
	validRetryMgr := NewRetryManager(validRetryConfig)

	cases := []struct {
		name     string
		retryMgr *RetryManager
		dialFn   func(ctx context.Context) (types.Socket, error)
		wantErr  error
	}{
		{
			name:     "nil dial func",
			retryMgr: validRetryMgr,
			wantErr:  ErrNilDialFunc,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			socket, err := AcquireSocket(
				context.Background(), tc.retryMgr, tc.dialFn, nil, nil)

			assert.Error(t, err)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Nil(t, socket)
		})
	}
}

func TestAcquireSocketContextCancellation(t *testing.T) {
	t.Run("context cancelled during sleep returns before next attempt", func(t *testing.T) {
		dialCount := atomic.Int32{}
		dial := func(ctx context.Context) (types.Socket, error) {
			dialCount.Add(1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			return nil, fmt.Errorf("dial failed")
		}

		ctx, cancel := context.WithCancel(context.Background())

		retryCfg, _ := NewRetryConfig(
			WithMaxRetries(10),
			WithInitialDelay(time.Second),
			WithMaxDelay(time.Second),
			WithJitterFactor(0.0),
		)
		retryMgr := NewRetryManager(retryCfg)

		done := make(chan error, 1)
		go func() {
			_, err := AcquireSocket(ctx, retryMgr, dial, nil, nil)
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

	t.Run("context cancelled during in-progress dial unblocks and returns", func(t *testing.T) {
		dial := func(ctx context.Context) (types.Socket, error) {
			// block until context is cancelled
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		ctx, cancel := context.WithCancel(context.Background())

		retryCfg, _ := NewRetryConfig()
		retryMgr := NewRetryManager(retryCfg)
		done := make(chan error, 1)
		go func() {
			_, err := AcquireSocket(ctx, retryMgr, dial, nil, nil)
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

func TestAcquireSocketDialErrors(t *testing.T) {
	t.Run("receive one error per failed dial", func(t *testing.T) {
		dialErr1 := errors.New("attempt 1 failed")
		dialErr2 := errors.New("attempt 2 failed")
		attemptIndex := atomic.Int32{}
		dial := func(ctx context.Context) (types.Socket, error) {
			i := attemptIndex.Add(1)
			switch i {
			case 1:
				return nil, dialErr1
			case 2:
				return nil, dialErr2
			default:
				return honeybeetest.NewMockSocket(), nil
			}
		}

		errCh := make(chan error, 2)

		retryCfg, _ := NewRetryConfig(
			WithMaxRetries(3),
			WithInitialDelay(1*time.Millisecond),
			WithMaxDelay(5*time.Millisecond),
			WithJitterFactor(0.0),
		)
		retryMgr := NewRetryManager(retryCfg)

		go func() {
			_, err := AcquireSocket(context.Background(), retryMgr, dial, errCh, nil)
			assert.NoError(t, err)
		}()

		var gotErrors []error
		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-errCh:
				gotErrors = append(gotErrors, err)
			default:
			}
			return len(gotErrors) == 2
		}, "expected errors")

		assert.Equal(t, dialErr1, gotErrors[0])
		assert.Equal(t, dialErr2, gotErrors[1])
	})

	t.Run("no errors on successful first dial", func(t *testing.T) {
		dial := func(ctx context.Context) (types.Socket, error) {
			return honeybeetest.NewMockSocket(), nil
		}

		errCh := make(chan error, 1)

		retryCfg, _ := NewRetryConfig(
			WithMaxRetries(3),
			WithInitialDelay(1*time.Millisecond),
			WithMaxDelay(5*time.Millisecond),
			WithJitterFactor(0.0),
		)
		retryMgr := NewRetryManager(retryCfg)

		_, err := AcquireSocket(context.Background(), retryMgr, dial, errCh, nil)
		assert.NoError(t, err)

		assert.Len(t, errCh, 0)
	})
}
