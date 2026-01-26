package honeybee

import (
	"net/url"

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
