// Package contracts provides data contract management
package contracts

import (
	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// Contract represents a data contract for a topic
type Contract struct {
	name        string
	version     *Version
	eventTypes  []string
	schemas     map[string]interface{}
	description string
}

// NewContract creates a new contract
func NewContract(name, version string) (*Contract, error) {
	v, err := ParseVersion(version)
	if err != nil {
		return nil, err
	}

	return &Contract{
		name:       name,
		version:    v,
		eventTypes: []string{},
		schemas:    make(map[string]interface{}),
	}, nil
}

// Name returns the contract name
func (c *Contract) Name() string {
	return c.name
}

// Version returns the contract version string
func (c *Contract) Version() string {
	return c.version.String()
}

// EventTypes returns all supported event types
func (c *Contract) EventTypes() []string {
	return c.eventTypes
}

// AddEventType adds an event type to the contract
func (c *Contract) AddEventType(eventType string, schema interface{}) error {
	if eventType == "" {
		return sdkerrors.NewValidationError("event_type", "cannot be empty", nil)
	}

	// Check if already exists
	for _, et := range c.eventTypes {
		if et == eventType {
			return sdkerrors.NewValidationError("event_type", "already exists in contract", nil)
		}
	}

	c.eventTypes = append(c.eventTypes, eventType)
	c.schemas[eventType] = schema
	return nil
}

// GetSchema returns the schema for a specific event type
func (c *Contract) GetSchema(eventType string) (interface{}, error) {
	schema, exists := c.schemas[eventType]
	if !exists {
		return nil, sdkerrors.ErrSchemaNotFound
	}
	return schema, nil
}

// IsCompatible checks if a message is compatible with this contract
func (c *Contract) IsCompatible(msg *core.BaseMessage) bool {
	// Check if event type is supported
	found := false
	for _, et := range c.eventTypes {
		if et == msg.EventType {
			found = true
			break
		}
	}

	if !found {
		return false
	}

	// Check schema version compatibility
	msgVersion, err := ParseVersion(msg.SchemaVersion)
	if err != nil {
		return false
	}

	// Compatible if major version matches
	return msgVersion.Major == c.version.Major
}

// SetDescription sets the contract description
func (c *Contract) SetDescription(description string) {
	c.description = description
}

// Description returns the contract description
func (c *Contract) Description() string {
	return c.description
}

// IsBreakingChange checks if upgrading from oldVersion to this version is breaking
func (c *Contract) IsBreakingChange(oldVersion string) (bool, error) {
	old, err := ParseVersion(oldVersion)
	if err != nil {
		return false, err
	}

	// Breaking if major version changed
	return c.version.Major != old.Major, nil
}

// SupportsEventType checks if an event type is supported
func (c *Contract) SupportsEventType(eventType string) bool {
	for _, et := range c.eventTypes {
		if et == eventType {
			return true
		}
	}
	return false
}

