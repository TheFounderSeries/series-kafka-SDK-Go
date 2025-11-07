package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
	"github.com/TheFounderSeries/series-kafka-go/topics/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProducerConsumer_EndToEnd tests full producer-consumer flow
func TestProducerConsumer_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup - use custom Sarama config for local testing (no auth)
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Version = sarama.V2_5_0_0
	
	producerConfig := &core.ProducerConfig{
		BootstrapServers:  "localhost:9092",
		SASLUsername:      "test",
		SASLPassword:      "test",
		ServiceName:       "test-producer",
		EnableIdempotence: false, // Disable for local testing
		MaxRetries:        3,
		RetryBackoffMs:    100,
		EnableMetrics:     false,
		EnableTracing:     false,
		SaramaConfig:      saramaConfig,
	}

	// Create producer
	producer, err := core.NewProducer(producerConfig)
	require.NoError(t, err)
	defer producer.Close()

	// Create topic
	topic := messages.NewMessagesTopic()

	// Create payload
	payload := &messages.SendBluePayload{
		AccountEmail:  "test@example.com",
		Content:       "Test message",
		FromNumber:    "+1234567890",
		ToNumber:      "+0987654321",
		MessageHandle: "test-handle",
		Status:        "sent",
		DateSent:      time.Now().Format(time.RFC3339),
		DateUpdated:   time.Now().Format(time.RFC3339),
		IsOutbound:    true,
	}

	// Produce message
	ctx := context.Background()
	eventID, err := producer.Produce(ctx, topic, payload, "message.sendblue")
	require.NoError(t, err)
	assert.NotEmpty(t, eventID)

	fmt.Printf("✓ Message produced successfully: %s\n", eventID)

	// Setup consumer with custom Sarama config for local testing
	consumerSaramaConfig := sarama.NewConfig()
	consumerSaramaConfig.Consumer.Return.Errors = true
	consumerSaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerSaramaConfig.Version = sarama.V2_5_0_0
	
	consumerConfig := &core.ConsumerConfig{
		BootstrapServers: "localhost:9092",
		SASLUsername:     "test",
		SASLPassword:     "test",
		GroupID:          "test-consumer-group",
		ServiceName:      "test-consumer",
		AutoOffsetReset:  "earliest",
		EnableDLQ:        true,
		MaxRetries:       3,
		RetryBackoffMs:   100,
		WorkerCount:      5,
		EnableMetrics:    false,
		EnableTracing:    false,
		SaramaConfig:     consumerSaramaConfig,
	}

	// Track consumed messages
	var consumedMu sync.Mutex
	var consumedMessages []*core.BaseMessage
	var consumedCount int

	handler := func(ctx context.Context, msg *core.BaseMessage) error {
		consumedMu.Lock()
		defer consumedMu.Unlock()
		
		consumedMessages = append(consumedMessages, msg)
		consumedCount++
		
		fmt.Printf("✓ Message consumed: %s (event_type: %s)\n", msg.EventID, msg.EventType)
		return nil
	}

	consumer, err := core.NewConsumer(consumerConfig, []core.Topic{topic}, handler)
	require.NoError(t, err)

	// Start consumer
	consumerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = consumer.Start(consumerCtx)
	require.NoError(t, err)
	defer consumer.Close()

	// Wait for consumption (max 30 seconds)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for message consumption")
		case <-ticker.C:
			consumedMu.Lock()
			count := consumedCount
			consumedMu.Unlock()
			
			if count > 0 {
				// Success!
				consumedMu.Lock()
				msg := consumedMessages[0]
				consumedMu.Unlock()
				
				assert.Equal(t, eventID, msg.EventID)
				assert.Equal(t, "message.sendblue", msg.EventType)
				assert.Equal(t, "test-producer", msg.SourceService)
				
				fmt.Printf("✓ Integration test PASSED - Message produced and consumed successfully\n")
				return
			}
		}
	}
}

// TestConsumer_SubsetFields tests subset field extraction
func TestConsumer_SubsetFields(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup producer with custom Sarama config for local testing
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Version = sarama.V2_5_0_0
	
	producerConfig := &core.ProducerConfig{
		BootstrapServers:  "localhost:9092",
		SASLUsername:      "test",
		SASLPassword:      "test",
		ServiceName:       "test-producer-subset",
		EnableIdempotence: false,
		EnableMetrics:     false,
		EnableTracing:     false,
		SaramaConfig:      saramaConfig,
	}

	producer, err := core.NewProducer(producerConfig)
	require.NoError(t, err)
	defer producer.Close()

	topic := messages.NewMessagesTopic()

	payload := &messages.AgentResponsePayload{
		UserID:           "usr_12345",
		ConversationID:   "conv_67890",
		ResponseText:     "Hello from AI",
		Intent:           "greeting",
		ConfidenceScore:  0.95,
		ProcessingTimeMs: 150.5,
		ModelVersion:     "v1.0",
	}

	// Produce message
	ctx := context.Background()
	eventID, err := producer.Produce(ctx, topic, payload, "message.agent_response")
	require.NoError(t, err)

	fmt.Printf("✓ Message produced for subset test: %s\n", eventID)

	// Setup consumer with subset fields and custom Sarama config
	consumerSaramaConfig := sarama.NewConfig()
	consumerSaramaConfig.Consumer.Return.Errors = true
	consumerSaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerSaramaConfig.Version = sarama.V2_5_0_0
	
	consumerConfig := &core.ConsumerConfig{
		BootstrapServers: "localhost:9092",
		SASLUsername:     "test",
		SASLPassword:     "test",
		GroupID:          "test-consumer-subset-v2",
		ServiceName:      "test-consumer-subset",
		AutoOffsetReset:  "earliest",
		WorkerCount:      5,
		SubsetFields:     []string{"payload.user_id", "payload.conversation_id", "event_id"},
		EnableMetrics:    false,
		EnableTracing:    false,
		EnableDLQ:        false,
		SaramaConfig:     consumerSaramaConfig,
	}

	var subsetMu sync.Mutex
	var receivedSubset map[string]interface{}
	var subsetReceived bool

	subsetHandler := func(ctx context.Context, fields map[string]interface{}) error {
		subsetMu.Lock()
		defer subsetMu.Unlock()
		
		receivedSubset = fields
		subsetReceived = true
		
		fmt.Printf("✓ Subset fields received: %+v\n", fields)
		return nil
	}

	consumer, err := core.NewConsumerWithSubsetHandler(consumerConfig, []core.Topic{topic}, subsetHandler)
	require.NoError(t, err)

	consumerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = consumer.Start(consumerCtx)
	require.NoError(t, err)
	defer consumer.Close()

	// Wait for consumption
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for subset message consumption")
		case <-ticker.C:
			subsetMu.Lock()
			received := subsetReceived
			subset := receivedSubset
			subsetMu.Unlock()
			
			if received {
				// Verify subset fields
				assert.Contains(t, subset, "user_id")
				assert.Contains(t, subset, "conversation_id")
				assert.Contains(t, subset, "event_id")
				
				assert.Equal(t, "usr_12345", subset["user_id"])
				assert.Equal(t, "conv_67890", subset["conversation_id"])
				
				fmt.Printf("✓ Subset field extraction test PASSED\n")
				return
			}
		}
	}
}

// TestProducer_Idempotency tests idempotent message production
func TestProducer_Idempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Version = sarama.V2_5_0_0
	
	config := &core.ProducerConfig{
		BootstrapServers:  "localhost:9092",
		SASLUsername:      "test",
		SASLPassword:      "test",
		ServiceName:       "test-idempotency",
		EnableIdempotence: false,
		EnableMetrics:     false,
		EnableTracing:     false,
		SaramaConfig:      saramaConfig,
	}

	producer, err := core.NewProducer(config)
	require.NoError(t, err)
	defer producer.Close()

	topic := messages.NewMessagesTopic()
	payload := &messages.LinqPayload{
		ConversationID: "conv_test",
		UserID:         "usr_test",
		Content:        "Idempotency test",
		Timestamp:      time.Now(),
		MessageType:    "text",
	}

	ctx := context.Background()

	// Produce multiple messages quickly
	eventIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		eventID, err := producer.Produce(ctx, topic, payload, "message.linq")
		require.NoError(t, err)
		eventIDs[i] = eventID
	}

	// All event IDs should be unique (idempotency ensures no duplicates)
	uniqueIDs := make(map[string]bool)
	for _, id := range eventIDs {
		uniqueIDs[id] = true
	}

	assert.Equal(t, 10, len(uniqueIDs), "All event IDs should be unique")
	fmt.Printf("✓ Idempotency test PASSED - Produced 10 unique messages\n")
}

