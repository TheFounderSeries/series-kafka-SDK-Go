// Package messages provides the contract for the messages topic
package messages

import (
	"github.com/TheFounderSeries/series-kafka-go/pkg/contracts"
	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
)

// MessagesContract defines the data contract for the messages topic
type MessagesContract struct {
	*contracts.Contract
}

// NewMessagesContract creates a new messages topic contract
func NewMessagesContract() *MessagesContract {
	contract, _ := contracts.NewContract("messages", "1.0.0")
	
	// Register event types and their schemas
	_ = contract.AddEventType("message.sendblue", &SendBluePayload{})
	_ = contract.AddEventType("message.linq", &LinqPayload{})
	_ = contract.AddEventType("message.agent_response", &AgentResponsePayload{})
	
	contract.SetDescription("Messages topic for SendBlue, Linq, and Agent responses")

	return &MessagesContract{
		Contract: contract,
	}
}

// IsCompatible checks if a message is compatible with this contract
func (mc *MessagesContract) IsCompatible(msg *core.BaseMessage) bool {
	return mc.Contract.IsCompatible(msg)
}

