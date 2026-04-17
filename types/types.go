package types

import (
	"net/http"
	"time"
)

type Dialer interface {
	Dial(urlStr string, requestHeader http.Header) (Socket, *http.Response, error)
}

type Socket interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error

	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetCloseHandler(h func(code int, text string) error)
}
