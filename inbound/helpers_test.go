package inbound

import (
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupTestConnection(t *testing.T) (
	conn *transport.Connection,
	socket *honeybeetest.MockSocket,
	incoming chan honeybeetest.MockIncomingData,
	outgoing chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	socket, incoming, outgoing = honeybeetest.SetupTestSocket(t)

	var err error
	conn, err = transport.NewConnectionFromSocket(socket, nil, nil)
	assert.NoError(t, err)
	return
}
