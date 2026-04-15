package honeybee

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestConnectionSend(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(*Connection)
		data        []byte
		wantErr     bool
		wantErrText string
	}{
		{
			name:  "send succeeds when open",
			setup: func(c *Connection) {},
			data:  []byte("test message"),
		},
		{
			name: "send fails when closed",
			setup: func(c *Connection) {
				c.Close()
			},
			data:        []byte("test"),
			wantErr:     true,
			wantErrText: "connection closed",
		},
		{
			name: "send fails when queue full",
			setup: func(c *Connection) {
				// Fill outgoing channel
				for i := 0; i < 100; i++ {
					c.outgoing <- []byte("filler")
				}
			},
			data:        []byte("overflow"),
			wantErr:     true,
			wantErrText: "outgoing queue full",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := NewConnection("ws://test", nil, nil)
			assert.NoError(t, err)

			tc.setup(conn)

			err = conn.Send(tc.data)

			if tc.wantErr {
				assert.Error(t, err)
				if tc.wantErrText != "" {
					assert.ErrorContains(t, err, tc.wantErrText)
				}
				return
			}

			assert.NoError(t, err)

			select {
			case sent := <-conn.outgoing:
				assert.Equal(t, tc.data, sent)
			default:
				t.Fatal("data not sent to outgoing channel")
			}
		})
	}
}

// Run with `go test -race` to ensure no race conditions occur
func TestConnectionSendConcurrent(t *testing.T) {
	conn, err := NewConnection("ws://test", nil, nil)
	assert.NoError(t, err)

	// continuously consume outgoing channel in background
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-conn.outgoing:
			case <-done:
				return
			}
		}
	}()
	defer close(done)

	// Send from multiple goroutines concurrently
	const goroutines = 5
	const messagesPerGoroutine = 10
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				data := []byte(fmt.Sprintf("msg-%d-%d", id, j))
				err := conn.Send(data)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()
}
