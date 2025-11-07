// Package core provides topic interface and contracts
package core

// Topic defines the interface that all topic plugins must implement
type Topic interface {
	// Name returns the Kafka topic name
	Name() string

	// SchemaRegistry returns a map of event_type to schema/payload type
	// The value can be a struct type or validator function
	SchemaRegistry() map[string]interface{}

	// GetPartitionKey extracts the partition key from a message for a given event type
	// Returns empty string if no specific partitioning is needed (random partition)
	GetPartitionKey(msg *BaseMessage, eventType string) string

	// ValidateMessage performs topic-specific validation on a message
	// Returns error if validation fails
	ValidateMessage(msg *BaseMessage) error

	// GetContract returns the data contract for this topic
	GetContract() TopicContract

	// DLQTopic returns the dead letter queue topic name for this topic
	// Default implementation: "{topic}.dlq"
	DLQTopic() string

	// EnableDLQ returns whether DLQ is enabled for this topic
	// Default: true
	EnableDLQ() bool
}

// TopicContract defines the data contract for a topic
type TopicContract interface {
	// Name returns the contract name
	Name() string

	// Version returns the current contract version
	Version() string

	// EventTypes returns all supported event types in this contract
	EventTypes() []string

	// IsCompatible checks if a message is compatible with this contract version
	IsCompatible(msg *BaseMessage) bool

	// GetSchema returns the schema for a specific event type
	GetSchema(eventType string) (interface{}, error)
}

// BaseTopic provides default implementations for common topic functionality
type BaseTopic struct {
	name           string
	schemaRegistry map[string]interface{}
	enableDLQ      bool
}

// NewBaseTopic creates a new base topic with default settings
func NewBaseTopic(name string, schemaRegistry map[string]interface{}) *BaseTopic {
	return &BaseTopic{
		name:           name,
		schemaRegistry: schemaRegistry,
		enableDLQ:      true,
	}
}

// Name returns the topic name
func (t *BaseTopic) Name() string {
	return t.name
}

// SchemaRegistry returns the schema registry
func (t *BaseTopic) SchemaRegistry() map[string]interface{} {
	return t.schemaRegistry
}

// DLQTopic returns the default DLQ topic name
func (t *BaseTopic) DLQTopic() string {
	return t.name + ".dlq"
}

// EnableDLQ returns whether DLQ is enabled
func (t *BaseTopic) EnableDLQ() bool {
	return t.enableDLQ
}

// SetEnableDLQ sets whether DLQ is enabled
func (t *BaseTopic) SetEnableDLQ(enable bool) {
	t.enableDLQ = enable
}

// GetPartitionKey provides default implementation (no specific partitioning)
func (t *BaseTopic) GetPartitionKey(msg *BaseMessage, eventType string) string {
	return "" // Empty string means random partition
}

// ValidateMessage provides default implementation (basic validation)
func (t *BaseTopic) ValidateMessage(msg *BaseMessage) error {
	return msg.Validate()
}

// GetContract provides default implementation (returns nil - override in implementations)
func (t *BaseTopic) GetContract() TopicContract {
	return nil
}

