// Package errors provides custom error types for the Series Kafka SDK
package errors

import (
	"errors"
	"fmt"
)

// Error types for categorization and handling
var (
	// ErrProducerNotStarted is returned when attempting to produce without starting the producer
	ErrProducerNotStarted = errors.New("producer not started")
	
	// ErrConsumerNotStarted is returned when attempting to consume without starting the consumer
	ErrConsumerNotStarted = errors.New("consumer not started")
	
	// ErrInvalidConfig is returned when configuration validation fails
	ErrInvalidConfig = errors.New("invalid configuration")
	
	// ErrSchemaValidation is returned when schema validation fails
	ErrSchemaValidation = errors.New("schema validation failed")
	
	// ErrSerialization is returned when message serialization fails
	ErrSerialization = errors.New("serialization failed")
	
	// ErrDeserialization is returned when message deserialization fails
	ErrDeserialization = errors.New("deserialization failed")
	
	// ErrInvalidEventType is returned when event type is malformed
	ErrInvalidEventType = errors.New("invalid event type format")
	
	// ErrTopicNotFound is returned when topic is not registered
	ErrTopicNotFound = errors.New("topic not found")
	
	// ErrSchemaNotFound is returned when schema is not registered for event type
	ErrSchemaNotFound = errors.New("schema not found for event type")
)

// ProducerError represents errors during message production
type ProducerError struct {
	Op      string // Operation that failed
	Topic   string // Topic name
	Message string // Error message
	Err     error  // Underlying error
}

func (e *ProducerError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("producer error [%s] on topic '%s': %s: %v", e.Op, e.Topic, e.Message, e.Err)
	}
	return fmt.Sprintf("producer error [%s] on topic '%s': %s", e.Op, e.Topic, e.Message)
}

func (e *ProducerError) Unwrap() error {
	return e.Err
}

// IsRetryable returns true if the error is transient and can be retried
func (e *ProducerError) IsRetryable() bool {
	// Network errors, temporary failures are retryable
	// Validation errors are not
	return e.Err != ErrSchemaValidation && e.Err != ErrInvalidEventType
}

// ConsumerError represents errors during message consumption
type ConsumerError struct {
	Op        string // Operation that failed
	Topic     string // Topic name
	Partition int32  // Partition number
	Offset    int64  // Message offset
	Message   string // Error message
	Err       error  // Underlying error
}

func (e *ConsumerError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("consumer error [%s] on topic '%s' partition %d offset %d: %s: %v", 
			e.Op, e.Topic, e.Partition, e.Offset, e.Message, e.Err)
	}
	return fmt.Sprintf("consumer error [%s] on topic '%s' partition %d offset %d: %s", 
		e.Op, e.Topic, e.Partition, e.Offset, e.Message)
}

func (e *ConsumerError) Unwrap() error {
	return e.Err
}

// IsRetryable returns true if the error is transient and can be retried
func (e *ConsumerError) IsRetryable() bool {
	// Handler errors are retryable, deserialization errors are not
	return e.Err != ErrDeserialization && e.Err != ErrSchemaValidation
}

// ValidationError represents schema or message validation errors
type ValidationError struct {
	Field   string // Field that failed validation
	Message string // Error message
	Err     error  // Underlying error
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation error: %s", e.Message)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// IsRetryable returns false as validation errors are permanent
func (e *ValidationError) IsRetryable() bool {
	return false
}

// DLQError represents errors when sending messages to DLQ
type DLQError struct {
	Topic   string // Original topic name
	DLQName string // DLQ topic name
	Message string // Error message
	Err     error  // Underlying error
}

func (e *DLQError) Error() string {
	return fmt.Sprintf("DLQ error sending from '%s' to '%s': %s: %v", e.Topic, e.DLQName, e.Message, e.Err)
}

func (e *DLQError) Unwrap() error {
	return e.Err
}

// NewProducerError creates a new ProducerError
func NewProducerError(op, topic, message string, err error) *ProducerError {
	return &ProducerError{
		Op:      op,
		Topic:   topic,
		Message: message,
		Err:     err,
	}
}

// NewConsumerError creates a new ConsumerError
func NewConsumerError(op, topic string, partition int32, offset int64, message string, err error) *ConsumerError {
	return &ConsumerError{
		Op:        op,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Message:   message,
		Err:       err,
	}
}

// NewValidationError creates a new ValidationError
func NewValidationError(field, message string, err error) *ValidationError {
	return &ValidationError{
		Field:   field,
		Message: message,
		Err:     err,
	}
}

// NewDLQError creates a new DLQError
func NewDLQError(topic, dlqName, message string, err error) *DLQError {
	return &DLQError{
		Topic:   topic,
		DLQName: dlqName,
		Message: message,
		Err:     err,
	}
}

