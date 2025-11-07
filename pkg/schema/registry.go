// Package schema provides schema registry functionality
package schema

import (
	"fmt"
	"sync"

	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// SchemaRegistry provides schema registration and retrieval
type SchemaRegistry struct {
	schemas map[string]interface{}
	mu      sync.RWMutex
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[string]interface{}),
	}
}

// RegisterSchema registers a schema for an event type
func (r *SchemaRegistry) RegisterSchema(eventType string, schema interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if eventType == "" {
		return sdkerrors.NewValidationError("event_type", "cannot be empty", nil)
	}

	if schema == nil {
		return sdkerrors.NewValidationError("schema", "cannot be nil", nil)
	}

	r.schemas[eventType] = schema
	return nil
}

// GetSchema retrieves a schema for an event type
func (r *SchemaRegistry) GetSchema(eventType string) (interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schema, exists := r.schemas[eventType]
	if !exists {
		return nil, sdkerrors.NewValidationError("event_type",
			fmt.Sprintf("schema not found for event type '%s'", eventType),
			sdkerrors.ErrSchemaNotFound)
	}

	return schema, nil
}

// HasSchema checks if a schema is registered for an event type
func (r *SchemaRegistry) HasSchema(eventType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.schemas[eventType]
	return exists
}

// ListEventTypes returns all registered event types
func (r *SchemaRegistry) ListEventTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	eventTypes := make([]string, 0, len(r.schemas))
	for et := range r.schemas {
		eventTypes = append(eventTypes, et)
	}
	return eventTypes
}

// UnregisterSchema removes a schema for an event type
func (r *SchemaRegistry) UnregisterSchema(eventType string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.HasSchema(eventType) {
		return sdkerrors.NewValidationError("event_type",
			fmt.Sprintf("schema not found for event type '%s'", eventType),
			sdkerrors.ErrSchemaNotFound)
	}

	delete(r.schemas, eventType)
	return nil
}

// Clear removes all registered schemas
func (r *SchemaRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.schemas = make(map[string]interface{})
}

// Count returns the number of registered schemas
func (r *SchemaRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.schemas)
}

