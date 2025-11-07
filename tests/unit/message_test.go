package unit

import (
	"testing"
	"time"

	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBaseMessage(t *testing.T) {
	tests := []struct {
		name          string
		eventType     string
		sourceService string
		payload       interface{}
		expectError   bool
	}{
		{
			name:          "valid message",
			eventType:     "user.created",
			sourceService: "test-service",
			payload:       map[string]interface{}{"user_id": "123"},
			expectError:   false,
		},
		{
			name:          "invalid event type - no dot",
			eventType:     "usercreated",
			sourceService: "test-service",
			payload:       map[string]interface{}{"user_id": "123"},
			expectError:   true,
		},
		{
			name:          "invalid event type - uppercase",
			eventType:     "User.Created",
			sourceService: "test-service",
			payload:       map[string]interface{}{"user_id": "123"},
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := core.NewBaseMessage(tt.eventType, tt.sourceService, tt.payload)
			
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, msg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, msg)
				assert.NotEmpty(t, msg.EventID)
				assert.Equal(t, tt.eventType, msg.EventType)
				assert.Equal(t, tt.sourceService, msg.SourceService)
				assert.Equal(t, tt.payload, msg.Payload)
				assert.Equal(t, "1.0.0", msg.SchemaVersion)
				assert.False(t, msg.Timestamp.IsZero())
			}
		})
	}
}

func TestBaseMessage_Validate(t *testing.T) {
	tests := []struct {
		name        string
		message     *core.BaseMessage
		expectError bool
	}{
		{
			name: "valid message",
			message: &core.BaseMessage{
				EventID:       "test-id",
				EventType:     "user.created",
				SchemaVersion: "1.0.0",
				Timestamp:     time.Now(),
				SourceService: "test-service",
				Payload:       map[string]interface{}{"test": "data"},
			},
			expectError: false,
		},
		{
			name: "missing event_id",
			message: &core.BaseMessage{
				EventID:       "",
				EventType:     "user.created",
				SchemaVersion: "1.0.0",
				Timestamp:     time.Now(),
				SourceService: "test-service",
				Payload:       map[string]interface{}{"test": "data"},
			},
			expectError: true,
		},
		{
			name: "invalid event_type format",
			message: &core.BaseMessage{
				EventID:       "test-id",
				EventType:     "invalid",
				SchemaVersion: "1.0.0",
				Timestamp:     time.Now(),
				SourceService: "test-service",
				Payload:       map[string]interface{}{"test": "data"},
			},
			expectError: true,
		},
		{
			name: "nil payload",
			message: &core.BaseMessage{
				EventID:       "test-id",
				EventType:     "user.created",
				SchemaVersion: "1.0.0",
				Timestamp:     time.Now(),
				SourceService: "test-service",
				Payload:       nil,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.message.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBaseMessage_WithMethods(t *testing.T) {
	msg, err := core.NewBaseMessage("test.event", "test-service", map[string]interface{}{"data": "test"})
	require.NoError(t, err)

	// Test WithCorrelationID
	msg.WithCorrelationID("corr-123")
	assert.NotNil(t, msg.CorrelationID)
	assert.Equal(t, "corr-123", *msg.CorrelationID)

	// Test WithTraceID
	msg.WithTraceID("trace-456")
	assert.NotNil(t, msg.TraceID)
	assert.Equal(t, "trace-456", *msg.TraceID)

	// Test WithParentSpanID
	msg.WithParentSpanID("span-789")
	assert.NotNil(t, msg.ParentSpanID)
	assert.Equal(t, "span-789", *msg.ParentSpanID)

	// Test WithMetadata
	msg.WithMetadata("key1", "value1")
	assert.Equal(t, "value1", msg.Metadata["key1"])
}

func TestBaseMessage_SetSchemaVersion(t *testing.T) {
	msg, err := core.NewBaseMessage("test.event", "test-service", map[string]interface{}{"data": "test"})
	require.NoError(t, err)

	// Valid version
	err = msg.SetSchemaVersion("2.1.0")
	assert.NoError(t, err)
	assert.Equal(t, "2.1.0", msg.SchemaVersion)

	// Invalid version
	err = msg.SetSchemaVersion("invalid")
	assert.Error(t, err)
}

