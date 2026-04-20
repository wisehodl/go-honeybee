package responderpool

import (
	"time"
)

type PoolEventKind string

const (
	EventPeerDisconnected PoolEventKind = "disconnected"
	EventPeerDropped      PoolEventKind = "dropped"
	EventPeerEvicted      PoolEventKind = "evicted"
)

type PoolEvent struct {
	ID   string
	Kind PoolEventKind
}

type InboxMessage struct {
	ID         string
	Data       []byte
	ReceivedAt time.Time
}
