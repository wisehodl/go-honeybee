package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseURL(t *testing.T) {
	type wantURL struct {
		scheme string
		host   string
		path   string
	}

	cases := []struct {
		name        string
		url         string
		want        wantURL
		wantErr     error
		wantErrText string
	}{
		{
			name: "valid ws url",
			url:  "ws://localhost:8080/relay",
			want: wantURL{
				scheme: "ws",
				host:   "localhost:8080",
				path:   "/relay",
			},
		},
		{
			name: "valid wss url",
			url:  "wss://relay.example.com",
			want: wantURL{
				scheme: "wss",
				host:   "relay.example.com",
				path:   "",
			},
		},
		{
			name:    "http scheme rejected",
			url:     "http://example.com",
			wantErr: errors.InvalidProtocol,
		},
		{
			name:    "missing scheme",
			url:     "example.com:8080",
			wantErr: errors.InvalidProtocol,
		},
		{
			name:    "empty string",
			url:     "",
			wantErr: errors.InvalidProtocol,
		},
		{
			name:        "malformed url",
			url:         "ws://[::1:8080",
			wantErrText: "missing ']' in host",
		},
		{
			name: "ipv6 address",
			url:  "ws://[::1]:8080/relay",
			want: wantURL{
				scheme: "ws",
				host:   "[::1]:8080",
				path:   "/relay",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseURL(tc.url)

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
			assert.Equal(t, tc.want.scheme, got.Scheme)
			assert.Equal(t, tc.want.host, got.Host)
			assert.Equal(t, tc.want.path, got.Path)
		})
	}
}
