package types

import (
	"context"
	"net/http"
	"time"
)

type Dialer interface {
	DialContext(ctx context.Context,
		url string,
		header http.Header,
	) (Socket, *http.Response, error)
}

type Socket interface {
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error

	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetCloseHandler(h func(code int, text string) error)
	SetPongHandler(h func(appData string) error)
}

type ReceivedMessage struct {
	Data       []byte
	ReceivedAt time.Time
}
