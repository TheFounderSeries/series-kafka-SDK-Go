// Package messages provides schemas for the messages topic
package messages

import (
	"time"
)

// SendBluePayload represents a SendBlue webhook message payload
type SendBluePayload struct {
	AccountEmail  string  `json:"account_email" required:"true"`
	Content       string  `json:"content" required:"true"`
	FromNumber    string  `json:"from_number" required:"true"`
	ToNumber      string  `json:"to_number" required:"true"`
	MessageHandle string  `json:"message_handle" required:"true"`
	Status        string  `json:"status" required:"true"`
	DateSent      string  `json:"date_sent" required:"true"`
	DateUpdated   string  `json:"date_updated" required:"true"`
	IsOutbound    bool    `json:"is_outbound"`
	MediaURL      *string `json:"media_url,omitempty"`
	ErrorCode     *string `json:"error_code,omitempty"`
}

// LinqPayload represents a Linq message payload
type LinqPayload struct {
	ConversationID string    `json:"conversation_id" required:"true"`
	UserID         string    `json:"user_id" required:"true"`
	Content        string    `json:"content" required:"true"`
	Timestamp      time.Time `json:"timestamp" required:"true"`
	MessageType    string    `json:"message_type" required:"true"` // "text", "image", etc.
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// AgentResponsePayload represents an AI agent response payload
type AgentResponsePayload struct {
	UserID           string  `json:"user_id" required:"true"`
	ConversationID   string  `json:"conversation_id" required:"true"`
	ResponseText     string  `json:"response_text" required:"true"`
	Intent           string  `json:"intent" required:"true"`
	ConfidenceScore  float64 `json:"confidence_score" required:"true"`
	ProcessingTimeMs float64 `json:"processing_time_ms" required:"true"`
	ModelVersion     string  `json:"model_version"`
}

// Validate methods

// Validate validates SendBluePayload
func (p *SendBluePayload) Validate() error {
	if p.AccountEmail == "" {
		return newFieldError("account_email", "cannot be empty")
	}
	if p.Content == "" {
		return newFieldError("content", "cannot be empty")
	}
	if p.FromNumber == "" {
		return newFieldError("from_number", "cannot be empty")
	}
	if p.ToNumber == "" {
		return newFieldError("to_number", "cannot be empty")
	}
	if p.MessageHandle == "" {
		return newFieldError("message_handle", "cannot be empty")
	}
	if p.Status == "" {
		return newFieldError("status", "cannot be empty")
	}
	return nil
}

// Validate validates LinqPayload
func (p *LinqPayload) Validate() error {
	if p.ConversationID == "" {
		return newFieldError("conversation_id", "cannot be empty")
	}
	if p.UserID == "" {
		return newFieldError("user_id", "cannot be empty")
	}
	if p.Content == "" {
		return newFieldError("content", "cannot be empty")
	}
	if p.Timestamp.IsZero() {
		return newFieldError("timestamp", "cannot be zero")
	}
	if p.MessageType == "" {
		return newFieldError("message_type", "cannot be empty")
	}
	return nil
}

// Validate validates AgentResponsePayload
func (p *AgentResponsePayload) Validate() error {
	if p.UserID == "" {
		return newFieldError("user_id", "cannot be empty")
	}
	if p.ConversationID == "" {
		return newFieldError("conversation_id", "cannot be empty")
	}
	if p.ResponseText == "" {
		return newFieldError("response_text", "cannot be empty")
	}
	if p.Intent == "" {
		return newFieldError("intent", "cannot be empty")
	}
	if p.ConfidenceScore < 0 || p.ConfidenceScore > 1 {
		return newFieldError("confidence_score", "must be between 0 and 1")
	}
	if p.ProcessingTimeMs < 0 {
		return newFieldError("processing_time_ms", "must be >= 0")
	}
	return nil
}

// Helper function for field errors
func newFieldError(field, message string) error {
	return &FieldError{Field: field, Message: message}
}

// FieldError represents a field validation error
type FieldError struct {
	Field   string
	Message string
}

func (e *FieldError) Error() string {
	return "field '" + e.Field + "': " + e.Message
}

