package transport

import (
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
			wantErr: InvalidProtocol,
		},
		{
			name:    "missing scheme",
			url:     "example.com:8080",
			wantErr: InvalidProtocol,
		},
		{
			name:    "empty string",
			url:     "",
			wantErr: InvalidProtocol,
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

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "strip trailing slash",
			input:    "wss://relay.example.com/",
			expected: "wss://relay.example.com",
		},
		{
			name:     "strip multiple trailing slashes",
			input:    "wss://relay.example.com//",
			expected: "wss://relay.example.com",
		},
		{
			name:     "strip trailing slash with path",
			input:    "wss://relay.example.com/path/",
			expected: "wss://relay.example.com/path",
		},
		{
			name:     "lowercase scheme",
			input:    "WSS://relay.example.com",
			expected: "wss://relay.example.com",
		},
		{
			name:     "lowercase host",
			input:    "wss://Relay.Example.Com",
			expected: "wss://relay.example.com",
		},
		{
			name:     "strip default wss port",
			input:    "wss://relay.example.com:443",
			expected: "wss://relay.example.com",
		},
		{
			name:     "strip default ws port",
			input:    "ws://relay.example.com:80",
			expected: "ws://relay.example.com",
		},
		{
			name:     "strip default wss port preserves ipv6 brackets",
			input:    "wss://[::1]:443",
			expected: "wss://[::1]",
		},
		{
			name:     "strip default ws port preserves ipv6 brackets",
			input:    "ws://[::1]:80",
			expected: "ws://[::1]",
		},
		{
			name:     "strip default port preserves ipv6 zone",
			input:    "wss://[fe80::1%25eth0]:443/",
			expected: "wss://[fe80::1%25eth0]",
		},
		{
			name:     "preserve non-default port",
			input:    "wss://relay.example.com:8080",
			expected: "wss://relay.example.com:8080",
		},
		{
			name:     "preserve path",
			input:    "wss://relay.example.com/nostr",
			expected: "wss://relay.example.com/nostr",
		},
		{
			name:     "lowercase scheme+host, preserve path case",
			input:    "Wss://Relay.Example.Com/Nostr",
			expected: "wss://relay.example.com/Nostr",
		},
		{
			name:  "preserve user info",
			input: "wss://user:P@ssWord@relay.example.com",
			// String() cannot output a raw `@` so `%40` is expected
			expected: "wss://user:P%40ssWord@relay.example.com",
		},
		{
			name:     "preserve query",
			input:    "Wss://relay.example.com?field=Value",
			expected: "wss://relay.example.com?field=Value",
		},
		{
			name:     "no change needed",
			input:    "wss://relay.example.com",
			expected: "wss://relay.example.com",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeURL(tc.input)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestNormalizeURLIdempotent(t *testing.T) {
	inputs := []string{
		"wss://[::1]:443",
		"ws://[::1]:80",
		"wss://[fe80::1%25eth0]:443/",
		"wss://relay.example.com:443/",
		"wss://relay.example.com:8080/path",
	}

	for _, input := range inputs {
		once, err := NormalizeURL(input)
		assert.NoError(t, err)
		twice, err := NormalizeURL(once)
		assert.NoError(t, err)
		assert.Equal(t, once, twice, "normalizing %q twice must be stable", input)
	}
}

func TestNormalizeURLError(t *testing.T) {
	_, err := NormalizeURL("http://relay.example.com")
	assert.ErrorIs(t, err, InvalidProtocol)
}
