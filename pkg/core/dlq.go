// Package core provides DLQ (Dead Letter Queue) handling
package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	jsoniter "github.com/json-iterator/go"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// DLQMessage represents the standardized error payload sent to DLQ
type DLQMessage struct {
	// Original message info
	OriginalEventID   string                 `json:"original_event_id"`
	OriginalEventType string                 `json:"original_event_type"`
	OriginalTopic     string                 `json:"original_topic"`
	OriginalPartition int32                  `json:"original_partition"`
	OriginalOffset    int64                  `json:"original_offset"`
	
	// Error information
	ErrorMessage   string    `json:"error_message"`
	ErrorTimestamp time.Time `json:"error_timestamp"`
	RetryCount     int       `json:"retry_count"`
	ServiceName    string    `json:"service_name"`
	
	// Context
	Context map[string]interface{} `json:"context,omitempty"`
	
	// Optional: original payload (can be omitted for large messages)
	OriginalPayload interface{} `json:"original_payload,omitempty"`
}

// DLQHandler handles sending messages to Dead Letter Queue topics
type DLQHandler struct {
	producer    sarama.SyncProducer
	serviceName string
	mu          sync.RWMutex
	started     bool
	
	// Track DLQ topics that have been created/verified
	verifiedTopics map[string]bool
	topicMu        sync.RWMutex
}

// NewDLQHandler creates a new DLQ handler
func NewDLQHandler(config *ProducerConfig) (*DLQHandler, error) {
	// Build Sarama config
	saramaConfig, err := config.BuildSaramaProducerConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build sarama config: %w", err)
	}

	// Create sync producer for DLQ (we want to ensure DLQ messages are delivered)
	brokers := parseBrokers(config.BootstrapServers)
	producer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		return nil, sdkerrors.NewProducerError("dlq_init", "", "failed to create DLQ producer", err)
	}

	return &DLQHandler{
		producer:       producer,
		serviceName:    config.ServiceName,
		started:        true,
		verifiedTopics: make(map[string]bool),
	}, nil
}

// SendToDLQ sends a failed message to the DLQ topic
func (h *DLQHandler) SendToDLQ(
	originalTopic string,
	dlqTopic string,
	partition int32,
	offset int64,
	eventID string,
	eventType string,
	errorMsg string,
	retryCount int,
	context map[string]interface{},
	payload interface{},
) error {
	h.mu.RLock()
	if !h.started {
		h.mu.RUnlock()
		return sdkerrors.NewDLQError(originalTopic, dlqTopic, "DLQ handler not started", nil)
	}
	h.mu.RUnlock()

	// Ensure DLQ topic exists (Kafka will auto-create if auto.create.topics.enable=true)
	if err := h.ensureDLQTopic(dlqTopic); err != nil {
		return err
	}

	// Create DLQ message
	dlqMsg := &DLQMessage{
		OriginalEventID:   eventID,
		OriginalEventType: eventType,
		OriginalTopic:     originalTopic,
		OriginalPartition: partition,
		OriginalOffset:    offset,
		ErrorMessage:      errorMsg,
		ErrorTimestamp:    time.Now().UTC(),
		RetryCount:        retryCount,
		ServiceName:       h.serviceName,
		Context:           context,
		OriginalPayload:   payload, // Include payload for debugging
	}

	// Serialize DLQ message
	msgBytes, err := json.Marshal(dlqMsg)
	if err != nil {
		return sdkerrors.NewDLQError(originalTopic, dlqTopic, "failed to serialize DLQ message", err)
	}

	// Send to DLQ topic
	msg := &sarama.ProducerMessage{
		Topic: dlqTopic,
		Value: sarama.ByteEncoder(msgBytes),
		Key:   sarama.StringEncoder(eventID), // Use original event ID as key for traceability
	}

	_, _, err = h.producer.SendMessage(msg)
	if err != nil {
		return sdkerrors.NewDLQError(originalTopic, dlqTopic, "failed to send message to DLQ", err)
	}

	return nil
}

// ensureDLQTopic verifies DLQ topic exists or attempts to create it
func (h *DLQHandler) ensureDLQTopic(dlqTopic string) error {
	// Check if we've already verified this topic
	h.topicMu.RLock()
	if h.verifiedTopics[dlqTopic] {
		h.topicMu.RUnlock()
		return nil
	}
	h.topicMu.RUnlock()

	// Note: In production, Kafka should have auto.create.topics.enable=true
	// or topics should be pre-created via IaC/admin tools.
	// For now, we'll just mark as verified and rely on auto-creation.
	h.topicMu.Lock()
	h.verifiedTopics[dlqTopic] = true
	h.topicMu.Unlock()

	return nil
}

// Close gracefully shuts down the DLQ handler
func (h *DLQHandler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.started {
		return nil
	}

	h.started = false
	return h.producer.Close()
}

// parseBrokers parses comma-separated broker addresses
func parseBrokers(bootstrapServers string) []string {
	brokers := []string{}
	for _, broker := range splitAndTrim(bootstrapServers, ",") {
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

// splitAndTrim splits a string by delimiter and trims whitespace
func splitAndTrim(s, sep string) []string {
	parts := []string{}
	for _, part := range splitString(s, sep) {
		trimmed := trimSpace(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

// Helper functions to avoid importing strings package
func splitString(s, sep string) []string {
	result := []string{}
	current := ""
	for _, char := range s {
		if string(char) == sep {
			result = append(result, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	result = append(result, current)
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	
	return s[start:end]
}

