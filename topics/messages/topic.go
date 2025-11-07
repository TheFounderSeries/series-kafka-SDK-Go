// Package messages provides the Messages topic implementation
package messages

import (
	"fmt"
	"sort"
	"strings"

	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
)

// MessagesTopic implements the Topic interface for the messages topic
type MessagesTopic struct {
	*core.BaseTopic
	contract *MessagesContract
}

// NewMessagesTopic creates a new Messages topic
func NewMessagesTopic() *MessagesTopic {
	schemaRegistry := map[string]interface{}{
		"message.sendblue":       &SendBluePayload{},
		"message.linq":           &LinqPayload{},
		"message.agent_response": &AgentResponsePayload{},
	}

	baseTopic := core.NewBaseTopic("messages", schemaRegistry)

	return &MessagesTopic{
		BaseTopic: baseTopic,
		contract:  NewMessagesContract(),
	}
}

// GetPartitionKey returns the partition key for a message
// Messages are partitioned by conversation to maintain order
func (t *MessagesTopic) GetPartitionKey(msg *core.BaseMessage, eventType string) string {
	switch eventType {
	case "message.sendblue":
		// Partition by conversation (phone number pair)
		if payload, ok := msg.Payload.(*SendBluePayload); ok {
			return getConversationKey(payload.FromNumber, payload.ToNumber)
		}
		// Try map access if not struct
		if payloadMap, ok := msg.Payload.(map[string]interface{}); ok {
			fromNumber, _ := payloadMap["from_number"].(string)
			toNumber, _ := payloadMap["to_number"].(string)
			return getConversationKey(fromNumber, toNumber)
		}

	case "message.linq":
		// Partition by conversation ID
		if payload, ok := msg.Payload.(*LinqPayload); ok {
			return payload.ConversationID
		}
		if payloadMap, ok := msg.Payload.(map[string]interface{}); ok {
			if convID, ok := payloadMap["conversation_id"].(string); ok {
				return convID
			}
		}

	case "message.agent_response":
		// Partition by user ID
		if payload, ok := msg.Payload.(*AgentResponsePayload); ok {
			return payload.UserID
		}
		if payloadMap, ok := msg.Payload.(map[string]interface{}); ok {
			if userID, ok := payloadMap["user_id"].(string); ok {
				return userID
			}
		}
	}

	return "" // Random partition if key cannot be extracted
}

// ValidateMessage performs topic-specific validation
func (t *MessagesTopic) ValidateMessage(msg *core.BaseMessage) error {
	// Base validation
	if err := msg.Validate(); err != nil {
		return err
	}

	// Validate event type is supported
	if !t.contract.SupportsEventType(msg.EventType) {
		return fmt.Errorf("unsupported event type '%s' for messages topic", msg.EventType)
	}

	// Payload-specific validation
	switch msg.EventType {
	case "message.sendblue":
		if payload, ok := msg.Payload.(*SendBluePayload); ok {
			return payload.Validate()
		}
	case "message.linq":
		if payload, ok := msg.Payload.(*LinqPayload); ok {
			return payload.Validate()
		}
	case "message.agent_response":
		if payload, ok := msg.Payload.(*AgentResponsePayload); ok {
			return payload.Validate()
		}
	}

	return nil
}

// GetContract returns the topic contract
func (t *MessagesTopic) GetContract() core.TopicContract {
	return t.contract
}

// getConversationKey creates a consistent key for a phone number conversation
// Sorts numbers to ensure bidirectional messages have the same key
func getConversationKey(num1, num2 string) string {
	numbers := []string{num1, num2}
	sort.Strings(numbers)
	return strings.Join(numbers, ":")
}

