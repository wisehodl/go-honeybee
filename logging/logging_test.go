package logging

import (
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	// "github.com/stretchr/testify/assert"
	"log/slog"
	"testing"
)

// Helpers

func log(level slog.Level, msg string, attrs map[string]any) honeybeetest.ExpectedLog {
	return honeybeetest.ExpectedLog{Level: level, Msg: msg, Attrs: attrs}
}

// Tests

func TestOutboundLogger(t *testing.T) {
	const POOL_ID = "pool-1"
	const PEER_ID = "wss://test"

	handler := honeybeetest.NewMockSlogHandler()
	poolLogger := NewOutboundPoolLogger(handler, POOL_ID)
	workerLogger := NewOutboundWorkerLogger(handler, POOL_ID, PEER_ID)
	connLogger := NewConnectionLogger(handler, POOL_ID, PEER_ID)

	poolLogger.Info("test")
	workerLogger.Info("test")
	connLogger.Info("test")

	honeybeetest.Eventually(t, func() bool {
		return len(handler.GetRecords()) == 3
	}, "expected a log record")

	records := handler.GetRecords()

	honeybeetest.AssertAttributePresent(t, records[0], KEY_MODULE, MODULE_NAME)
	honeybeetest.AssertAttributePresent(t, records[0], KEY_COMPONENT, COMPONENT_OUTBOUND_POOL)
	honeybeetest.AssertAttributePresent(t, records[0], KEY_POOL_ID, POOL_ID)

	honeybeetest.AssertAttributePresent(t, records[1], KEY_MODULE, MODULE_NAME)
	honeybeetest.AssertAttributePresent(t, records[1], KEY_COMPONENT, COMPONENT_OUTBOUND_WORKER)
	honeybeetest.AssertAttributePresent(t, records[1], KEY_POOL_ID, POOL_ID)
	honeybeetest.AssertAttributePresent(t, records[1], KEY_PEER_ID, PEER_ID)

	honeybeetest.AssertAttributePresent(t, records[2], KEY_MODULE, MODULE_NAME)
	honeybeetest.AssertAttributePresent(t, records[2], KEY_COMPONENT, COMPONENT_CONNECTION)
	honeybeetest.AssertAttributePresent(t, records[2], KEY_POOL_ID, POOL_ID)
	honeybeetest.AssertAttributePresent(t, records[2], KEY_PEER_ID, PEER_ID)
}

func TestInboundLogger(t *testing.T) {
	const POOL_ID = "pool-1"
	const PEER_ID = "peer-1"

	handler := honeybeetest.NewMockSlogHandler()
	poolLogger := NewInboundPoolLogger(handler, POOL_ID)
	workerLogger := NewInboundWorkerLogger(handler, POOL_ID, PEER_ID)
	connLogger := NewConnectionLogger(handler, POOL_ID, PEER_ID)

	poolLogger.Info("test")
	workerLogger.Info("test")
	connLogger.Info("test")

	honeybeetest.Eventually(t, func() bool {
		return len(handler.GetRecords()) == 3
	}, "expected a log record")

	records := handler.GetRecords()

	honeybeetest.AssertAttributePresent(t, records[0], KEY_MODULE, MODULE_NAME)
	honeybeetest.AssertAttributePresent(t, records[0], KEY_COMPONENT, COMPONENT_INBOUND_POOL)
	honeybeetest.AssertAttributePresent(t, records[0], KEY_POOL_ID, POOL_ID)

	honeybeetest.AssertAttributePresent(t, records[1], KEY_MODULE, MODULE_NAME)
	honeybeetest.AssertAttributePresent(t, records[1], KEY_COMPONENT, COMPONENT_INBOUND_WORKER)
	honeybeetest.AssertAttributePresent(t, records[1], KEY_POOL_ID, POOL_ID)
	honeybeetest.AssertAttributePresent(t, records[1], KEY_PEER_ID, PEER_ID)

	honeybeetest.AssertAttributePresent(t, records[2], KEY_MODULE, MODULE_NAME)
	honeybeetest.AssertAttributePresent(t, records[2], KEY_COMPONENT, COMPONENT_CONNECTION)
	honeybeetest.AssertAttributePresent(t, records[2], KEY_POOL_ID, POOL_ID)
	honeybeetest.AssertAttributePresent(t, records[2], KEY_PEER_ID, PEER_ID)
}
