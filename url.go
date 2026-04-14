package honeybee

import (
	"net/url"
	"strings"

	"git.wisehodl.dev/jay/go-honeybee/errors"
)

func ParseURL(urlStr string) (*url.URL, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	if parsedURL.Scheme != "ws" && parsedURL.Scheme != "wss" {
		return nil, errors.InvalidProtocol
	}

	return parsedURL, nil
}

func NormalizeURL(input string) (string, error) {
	parsed, err := ParseURL(strings.ToLower(input))
	if err != nil {
		return "", err
	}

	host := parsed.Hostname()
	port := parsed.Port()
	if (parsed.Scheme == "wss" && port == "443") ||
		(parsed.Scheme == "ws" && port == "80") {
		parsed.Host = host
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/")

	return parsed.String(), nil

}
