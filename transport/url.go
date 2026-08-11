package transport

import (
	"errors"
	"net/url"
	"strings"
)

var InvalidProtocol = errors.New("URL must use ws:// or wss:// scheme")

func ParseURL(urlStr string) (*url.URL, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	if parsedURL.Scheme != "ws" && parsedURL.Scheme != "wss" {
		return nil, InvalidProtocol
	}

	return parsedURL, nil
}

func NormalizeURL(input string) (string, error) {
	parsed, err := ParseURL(input)
	if err != nil {
		return "", err
	}

	// lowercase host
	parsed.Host = strings.ToLower(parsed.Host)

	port := parsed.Port()

	// strip default ports by suffix-trim so ipv6 brackets and zone survive
	if (parsed.Scheme == "wss" && port == "443") ||
		(parsed.Scheme == "ws" && port == "80") {
		parsed.Host = strings.TrimSuffix(parsed.Host, ":"+port)
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/")

	return parsed.String(), nil

}
