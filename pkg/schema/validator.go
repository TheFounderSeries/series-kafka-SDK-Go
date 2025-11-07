// Package schema provides validation functionality
package schema

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// Validator provides schema validation for messages and payloads
type Validator struct {
	eventTypePattern *regexp.Regexp
	versionPattern   *regexp.Regexp
}

// NewValidator creates a new validator
func NewValidator() *Validator {
	return &Validator{
		eventTypePattern: regexp.MustCompile(`^[a-z_]+\.[a-z_]+$`),
		versionPattern:   regexp.MustCompile(`^\d+\.\d+\.\d+$`),
	}
}

// ValidateMessage validates a complete message
func (v *Validator) ValidateMessage(msg *core.BaseMessage) error {
	return msg.Validate()
}

// ValidateEventType validates event type format
func (v *Validator) ValidateEventType(eventType string) error {
	if !v.eventTypePattern.MatchString(eventType) {
		return sdkerrors.NewValidationError("event_type",
			"must match pattern '^[a-z_]+\\.[a-z_]+$' (e.g., 'user.created')",
			sdkerrors.ErrInvalidEventType)
	}
	return nil
}

// ValidateSchemaVersion validates schema version format (semantic versioning)
func (v *Validator) ValidateSchemaVersion(version string) error {
	if !v.versionPattern.MatchString(version) {
		return sdkerrors.NewValidationError("schema_version",
			"must match semantic versioning (e.g., '1.0.0')",
			nil)
	}
	return nil
}

// ValidatePayload validates a payload against expected schema/type
func (v *Validator) ValidatePayload(payload interface{}, expectedType interface{}) error {
	if payload == nil {
		return sdkerrors.NewValidationError("payload", "cannot be nil", nil)
	}

	// If expectedType is a reflect.Type, check type compatibility
	if typ, ok := expectedType.(reflect.Type); ok {
		payloadType := reflect.TypeOf(payload)
		if payloadType != typ {
			return sdkerrors.NewValidationError("payload",
				fmt.Sprintf("expected type %s, got %s", typ, payloadType),
				sdkerrors.ErrSchemaValidation)
		}
	}

	return nil
}

// ValidateTopicMessage validates a message against a topic's schema registry
func (v *Validator) ValidateTopicMessage(msg *core.BaseMessage, topic core.Topic) error {
	// Validate base message
	if err := msg.Validate(); err != nil {
		return err
	}

	// Validate event type is registered in topic
	schemaRegistry := topic.SchemaRegistry()
	if _, exists := schemaRegistry[msg.EventType]; !exists {
		return sdkerrors.NewValidationError("event_type",
			fmt.Sprintf("event type '%s' not registered in topic '%s'", msg.EventType, topic.Name()),
			sdkerrors.ErrSchemaNotFound)
	}

	// Run topic-specific validation
	if err := topic.ValidateMessage(msg); err != nil {
		return err
	}

	return nil
}

// ValidateStructTags validates a struct using reflection and struct tags
// This is useful for runtime validation of payload structs
func (v *Validator) ValidateStructTags(payload interface{}) error {
	val := reflect.ValueOf(payload)
	
	// Dereference pointers
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return sdkerrors.NewValidationError("payload", "cannot validate nil pointer", nil)
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil // Only validate structs
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Check required tag
		if field.Tag.Get("required") == "true" {
			if isZeroValue(fieldVal) {
				return sdkerrors.NewValidationError(field.Name,
					"required field is missing or zero value",
					nil)
			}
		}

		// Recursively validate nested structs
		if fieldVal.Kind() == reflect.Struct {
			if err := v.ValidateStructTags(fieldVal.Interface()); err != nil {
				return err
			}
		}
	}

	return nil
}

// isZeroValue checks if a reflect.Value is the zero value for its type
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

