// Package schema provides serialization and schema validation
package schema

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Serializer handles message serialization and deserialization
type Serializer struct{}

// NewSerializer creates a new serializer
func NewSerializer() *Serializer {
	return &Serializer{}
}

// Serialize converts a BaseMessage to JSON bytes
func (s *Serializer) Serialize(msg *core.BaseMessage) ([]byte, error) {
	if msg == nil {
		return nil, sdkerrors.NewValidationError("message", "cannot serialize nil message", nil)
	}

	// Validate before serialization
	if err := msg.Validate(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, sdkerrors.ErrSerialization
	}

	return data, nil
}

// Deserialize converts JSON bytes to a BaseMessage
func (s *Serializer) Deserialize(data []byte) (*core.BaseMessage, error) {
	if len(data) == 0 {
		return nil, sdkerrors.NewValidationError("data", "cannot deserialize empty data", nil)
	}

	var msg core.BaseMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, sdkerrors.ErrDeserialization
	}

	// Validate after deserialization
	if err := msg.Validate(); err != nil {
		return nil, err
	}

	return &msg, nil
}

// SerializeToString serializes a message to a JSON string
func (s *Serializer) SerializeToString(msg *core.BaseMessage) (string, error) {
	data, err := s.Serialize(msg)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// DeserializeFromString deserializes a message from a JSON string
func (s *Serializer) DeserializeFromString(data string) (*core.BaseMessage, error) {
	return s.Deserialize([]byte(data))
}

