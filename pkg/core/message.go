// Package core provides core message types and functionality
package core

import (
	"regexp"
	"time"

	"github.com/google/uuid"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// BaseMessage represents the universal message envelope for all events.
// This structure matches the Python SDK specification exactly.
type BaseMessage struct {
	// Event identity
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"` // Format: "entity.action" (e.g., "user.created")

	// Versioning
	SchemaVersion string `json:"schema_version"` // Semantic version (e.g., "1.0.0")

	// Metadata
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`

	// Payload - the actual event data (type depends on event_type)
	Payload interface{} `json:"payload"`

	// Distributed tracing
	CorrelationID *string `json:"correlation_id,omitempty"`
	TraceID       *string `json:"trace_id,omitempty"`
	ParentSpanID  *string `json:"parent_span_id,omitempty"`

	// Additional metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// eventTypePattern validates event_type format: "entity.action"
var eventTypePattern = regexp.MustCompile(`^[a-z_]+\.[a-z_]+$`)

// schemaVersionPattern validates semantic versioning: "major.minor.patch"
var schemaVersionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

// NewBaseMessage creates a new BaseMessage with auto-generated EventID and timestamp
func NewBaseMessage(eventType, sourceService string, payload interface{}) (*BaseMessage, error) {
	// Validate event type format
	if !eventTypePattern.MatchString(eventType) {
		return nil, sdkerrors.NewValidationError("event_type", 
			"must match pattern '^[a-z_]+\\.[a-z_]+$' (e.g., 'user.created')", 
			sdkerrors.ErrInvalidEventType)
	}

	return &BaseMessage{
		EventID:       uuid.New().String(),
		EventType:     eventType,
		SchemaVersion: "1.0.0", // Default version
		Timestamp:     time.Now().UTC(),
		SourceService: sourceService,
		Payload:       payload,
		Metadata:      make(map[string]interface{}),
	}, nil
}

// Validate performs validation on the message fields
func (m *BaseMessage) Validate() error {
	// Validate event_id (must be present)
	if m.EventID == "" {
		return sdkerrors.NewValidationError("event_id", "cannot be empty", nil)
	}

	// Validate event_type format
	if !eventTypePattern.MatchString(m.EventType) {
		return sdkerrors.NewValidationError("event_type", 
			"must match pattern '^[a-z_]+\\.[a-z_]+$'", 
			sdkerrors.ErrInvalidEventType)
	}

	// Validate schema_version format
	if m.SchemaVersion != "" && !schemaVersionPattern.MatchString(m.SchemaVersion) {
		return sdkerrors.NewValidationError("schema_version", 
			"must match semantic versioning (e.g., '1.0.0')", nil)
	}

	// Validate source_service (must be present)
	if m.SourceService == "" {
		return sdkerrors.NewValidationError("source_service", "cannot be empty", nil)
	}

	// Validate payload (must be present)
	if m.Payload == nil {
		return sdkerrors.NewValidationError("payload", "cannot be nil", nil)
	}

	// Validate timestamp
	if m.Timestamp.IsZero() {
		return sdkerrors.NewValidationError("timestamp", "cannot be zero", nil)
	}

	return nil
}

// WithCorrelationID sets the correlation ID for request tracing
func (m *BaseMessage) WithCorrelationID(correlationID string) *BaseMessage {
	m.CorrelationID = &correlationID
	return m
}

// WithTraceID sets the trace ID for distributed tracing
func (m *BaseMessage) WithTraceID(traceID string) *BaseMessage {
	m.TraceID = &traceID
	return m
}

// WithParentSpanID sets the parent span ID for distributed tracing
func (m *BaseMessage) WithParentSpanID(parentSpanID string) *BaseMessage {
	m.ParentSpanID = &parentSpanID
	return m
}

// WithMetadata adds a metadata key-value pair
func (m *BaseMessage) WithMetadata(key string, value interface{}) *BaseMessage {
	if m.Metadata == nil {
		m.Metadata = make(map[string]interface{})
	}
	m.Metadata[key] = value
	return m
}

// SetSchemaVersion sets a custom schema version
func (m *BaseMessage) SetSchemaVersion(version string) error {
	if !schemaVersionPattern.MatchString(version) {
		return sdkerrors.NewValidationError("schema_version", 
			"must match semantic versioning (e.g., '1.0.0')", nil)
	}
	m.SchemaVersion = version
	return nil
}

