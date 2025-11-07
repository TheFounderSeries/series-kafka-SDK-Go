// Package core provides Kafka consumer implementation
package core

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// MessageHandler processes full messages
type MessageHandler func(context.Context, *BaseMessage) error

// SubsetMessageHandler processes subset field extractions
type SubsetMessageHandler func(context.Context, map[string]interface{}) error

// Consumer provides a Kafka consumer with DLQ, retry, and subset field support
type Consumer struct {
	config        *ConsumerConfig
	topics        map[string]Topic
	handler       MessageHandler
	subsetHandler SubsetMessageHandler
	dlqHandlers   map[string]*DLQHandler
	
	consumerGroup sarama.ConsumerGroup
	ready         chan bool
	mu            sync.RWMutex
	started       bool
	
	// OpenTelemetry
	tracer            trace.Tracer
	meter             metric.Meter
	messagesConsumed  metric.Int64Counter
	handlerLatency    metric.Float64Histogram
	handlerErrors     metric.Int64Counter
	dlqMessages       metric.Int64Counter
	retries           metric.Int64Counter
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(
	config *ConsumerConfig,
	topics []Topic,
	handler MessageHandler,
) (*Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Build topic map
	topicMap := make(map[string]Topic)
	for _, topic := range topics {
		topicMap[topic.Name()] = topic
	}

	c := &Consumer{
		config:      config,
		topics:      topicMap,
		handler:     handler,
		dlqHandlers: make(map[string]*DLQHandler),
		ready:       make(chan bool),
		started:     false,
	}

	// Initialize OpenTelemetry if enabled
	if config.EnableTracing {
		c.tracer = otel.Tracer("series-kafka-consumer")
	}

	if config.EnableMetrics {
		c.meter = otel.Meter("series-kafka-consumer")
		if err := c.setupMetrics(); err != nil {
			fmt.Printf("Warning: failed to setup metrics: %v\n", err)
		}
	}

	return c, nil
}

// NewConsumerWithSubsetHandler creates a consumer for subset field extraction
func NewConsumerWithSubsetHandler(
	config *ConsumerConfig,
	topics []Topic,
	subsetHandler SubsetMessageHandler,
) (*Consumer, error) {
	if len(config.SubsetFields) == 0 {
		return nil, sdkerrors.NewValidationError("subset_fields", 
			"subset_fields must be configured when using SubsetMessageHandler", nil)
	}

	c, err := NewConsumer(config, topics, nil)
	if err != nil {
		return nil, err
	}

	c.subsetHandler = subsetHandler
	return c, nil
}

// setupMetrics initializes OpenTelemetry metrics
func (c *Consumer) setupMetrics() error {
	var err error

	c.messagesConsumed, err = c.meter.Int64Counter(
		"kafka.consumer.messages.consumed",
		metric.WithDescription("Total number of messages consumed"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	c.handlerLatency, err = c.meter.Float64Histogram(
		"kafka.consumer.handler.latency",
		metric.WithDescription("Message handler latency"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	c.handlerErrors, err = c.meter.Int64Counter(
		"kafka.consumer.handler.errors",
		metric.WithDescription("Total number of handler errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	c.dlqMessages, err = c.meter.Int64Counter(
		"kafka.consumer.dlq.messages",
		metric.WithDescription("Total number of messages sent to DLQ"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	c.retries, err = c.meter.Int64Counter(
		"kafka.consumer.retries",
		metric.WithDescription("Total number of message processing retries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	return nil
}

// Start initializes and starts the consumer
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// Initialize DLQ handlers for topics that have DLQ enabled
	for _, topic := range c.topics {
		if topic.EnableDLQ() && c.config.EnableDLQ {
			dlqConfig := &ProducerConfig{
				BootstrapServers:  c.config.BootstrapServers,
				SASLUsername:      c.config.SASLUsername,
				SASLPassword:      c.config.SASLPassword,
				ServiceName:       c.config.ServiceName,
				EnableIdempotence: false,
				MaxRetries:        3,
				RetryBackoffMs:    100,
			}
			
			// Use consumer's custom Sarama config if provided (but clone it for DLQ producer)
			if c.config.SaramaConfig != nil {
				// Clone consumer config for producer
				producerConfig := sarama.NewConfig()
				producerConfig.Version = c.config.SaramaConfig.Version
				producerConfig.Producer.Return.Successes = true
				producerConfig.Producer.Return.Errors = true
				producerConfig.Producer.RequiredAcks = sarama.WaitForAll
				producerConfig.Producer.Retry.Max = 3
				dlqConfig.SaramaConfig = producerConfig
			}
			
			dlqHandler, err := NewDLQHandler(dlqConfig)
			if err != nil {
				return fmt.Errorf("failed to create DLQ handler for topic %s: %w", topic.Name(), err)
			}
			
			c.dlqHandlers[topic.Name()] = dlqHandler
		}
	}

	// Build Sarama config
	saramaConfig, err := c.config.BuildSaramaConsumerConfig()
	if err != nil {
		return fmt.Errorf("failed to build sarama config: %w", err)
	}

	// Create consumer group
	brokers := parseBrokers(c.config.BootstrapServers)
	topicNames := make([]string, 0, len(c.topics))
	for name := range c.topics {
		topicNames = append(topicNames, name)
	}

	consumerGroup, err := sarama.NewConsumerGroup(brokers, c.config.GroupID, saramaConfig)
	if err != nil {
		return sdkerrors.NewConsumerError("init", "", 0, 0, "failed to create consumer group", err)
	}

	c.mu.Lock()
	c.consumerGroup = consumerGroup
	c.started = true
	c.mu.Unlock()

	// Start consuming in background
	go func() {
		for {
			// Check if context is cancelled
			if ctx.Err() != nil {
				return
			}

			// Consume with handler
			if err := consumerGroup.Consume(ctx, topicNames, c); err != nil {
				fmt.Printf("Error from consumer: %v\n", err)
			}

			// Check if consumer should stop
			c.mu.RLock()
			if !c.started {
				c.mu.RUnlock()
				return
			}
			c.mu.RUnlock()
		}
	}()

	// Wait for consumer to be ready
	<-c.ready

	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from a partition
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Create worker pool
	workerCh := make(chan *sarama.ConsumerMessage, c.config.WorkerCount)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < c.config.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range workerCh {
				c.processMessage(session.Context(), msg, session, claim)
			}
		}()
	}

	// Feed messages to workers
	for message := range claim.Messages() {
		workerCh <- message
	}

	// Close worker channel and wait for completion
	close(workerCh)
	wg.Wait()

	return nil
}

// processMessage handles a single message with retry and DLQ logic
func (c *Consumer) processMessage(
	ctx context.Context,
	kafkaMsg *sarama.ConsumerMessage,
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) {
	startTime := time.Now()
	topic := c.topics[kafkaMsg.Topic]

	// Start tracing span if enabled
	var span trace.Span
	if c.config.EnableTracing && c.tracer != nil {
		ctx, span = c.tracer.Start(ctx, "kafka.consume",
			trace.WithAttributes(
				attribute.String("kafka.topic", kafkaMsg.Topic),
				attribute.Int64("kafka.partition", int64(kafkaMsg.Partition)),
				attribute.Int64("kafka.offset", kafkaMsg.Offset),
			),
		)
		defer span.End()
	}

	// Deserialize message
	var rawMsg map[string]interface{}
	if err := json.Unmarshal(kafkaMsg.Value, &rawMsg); err != nil {
		c.sendToDLQ(kafkaMsg, topic, "deserialization_error", err, 0, nil)
		session.MarkMessage(kafkaMsg, "")
		return
	}

	eventType, _ := rawMsg["event_type"].(string)
	eventID, _ := rawMsg["event_id"].(string)

	// Event type filtering
	if len(c.config.EventTypeFilter) > 0 {
		found := false
		for _, et := range c.config.EventTypeFilter {
			if et == eventType {
				found = true
				break
			}
		}
		if !found {
			// Skip this message - not in filter
			session.MarkMessage(kafkaMsg, "")
			return
		}
	}

	// Parse full message or extract subset fields
	var handlerErr error
	if len(c.config.SubsetFields) > 0 && c.subsetHandler != nil {
		// Subset field extraction mode
		subset := extractSubsetFields(rawMsg, c.config.SubsetFields)
		handlerErr = c.invokeHandlerWithRetry(ctx, nil, subset, kafkaMsg, topic, eventType, eventID)
	} else {
		// Full message mode
		msg, err := c.deserializeMessage(rawMsg, topic, eventType)
		if err != nil {
			c.sendToDLQ(kafkaMsg, topic, "schema_validation_error", err, 0, rawMsg)
			session.MarkMessage(kafkaMsg, "")
			return
		}
		handlerErr = c.invokeHandlerWithRetry(ctx, msg, nil, kafkaMsg, topic, eventType, eventID)
	}

	// Record metrics
	latencyMs := float64(time.Since(startTime).Milliseconds())
	if handlerErr == nil {
		c.recordSuccess(ctx, kafkaMsg.Topic, latencyMs)
		session.MarkMessage(kafkaMsg, "")
	} else {
		c.recordError(ctx, kafkaMsg.Topic)
		// Message will be sent to DLQ by invokeHandlerWithRetry
		session.MarkMessage(kafkaMsg, "")
	}
}

// invokeHandlerWithRetry invokes the handler with exponential backoff retry
func (c *Consumer) invokeHandlerWithRetry(
	ctx context.Context,
	msg *BaseMessage,
	subset map[string]interface{},
	kafkaMsg *sarama.ConsumerMessage,
	topic Topic,
	eventType, eventID string,
) error {
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Record retry metric
			c.recordRetry(ctx, kafkaMsg.Topic)
			
			// Exponential backoff
			backoffMs := c.config.RetryBackoffMs * (1 << uint(attempt-1))
			time.Sleep(time.Duration(backoffMs) * time.Millisecond)
		}

		// Invoke appropriate handler
		var err error
		if subset != nil {
			err = c.subsetHandler(ctx, subset)
		} else {
			err = c.handler(ctx, msg)
		}

		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Check if error is retryable
		if consumerErr, ok := err.(*sdkerrors.ConsumerError); ok {
			if !consumerErr.IsRetryable() {
				break // Non-retryable error, stop retrying
			}
		}
	}

	// Max retries exceeded, send to DLQ
	var payload interface{}
	if msg != nil {
		payload = msg.Payload
	} else {
		payload = subset
	}
	
	c.sendToDLQ(kafkaMsg, topic, "handler_error", lastErr, c.config.MaxRetries, payload)
	return lastErr
}

// deserializeMessage deserializes and validates a message
func (c *Consumer) deserializeMessage(
	rawMsg map[string]interface{},
	topic Topic,
	eventType string,
) (*BaseMessage, error) {
	// Get schema for event type
	schemaRegistry := topic.SchemaRegistry()
	if _, exists := schemaRegistry[eventType]; !exists {
		return nil, sdkerrors.NewValidationError("event_type",
			fmt.Sprintf("unknown event type '%s' for topic '%s'", eventType, topic.Name()),
			sdkerrors.ErrSchemaNotFound)
	}

	// Reconstruct BaseMessage
	msg := &BaseMessage{
		EventID:       getString(rawMsg, "event_id"),
		EventType:     getString(rawMsg, "event_type"),
		SchemaVersion: getString(rawMsg, "schema_version"),
		SourceService: getString(rawMsg, "source_service"),
		Payload:       rawMsg["payload"],
		Metadata:      getMap(rawMsg, "metadata"),
	}

	// Parse timestamp
	if tsStr := getString(rawMsg, "timestamp"); tsStr != "" {
		ts, err := time.Parse(time.RFC3339, tsStr)
		if err == nil {
			msg.Timestamp = ts
		}
	}

	// Optional fields
	if corrID := getString(rawMsg, "correlation_id"); corrID != "" {
		msg.CorrelationID = &corrID
	}
	if traceID := getString(rawMsg, "trace_id"); traceID != "" {
		msg.TraceID = &traceID
	}
	if spanID := getString(rawMsg, "parent_span_id"); spanID != "" {
		msg.ParentSpanID = &spanID
	}

	// Validate
	if err := msg.Validate(); err != nil {
		return nil, err
	}

	return msg, nil
}

// extractSubsetFields extracts specified fields from a message using dot notation
func extractSubsetFields(rawMsg map[string]interface{}, paths []string) map[string]interface{} {
	result := make(map[string]interface{})

	for _, path := range paths {
		parts := strings.Split(path, ".")
		value := extractNestedValue(rawMsg, parts)
		
		if value != nil {
			// Use the last part of the path as the key
			key := parts[len(parts)-1]
			result[key] = value
		}
	}

	return result
}

// extractNestedValue extracts a value from nested maps using path parts
func extractNestedValue(data interface{}, parts []string) interface{} {
	if len(parts) == 0 {
		return data
	}

	// Use reflection to handle both maps and structs
	val := reflect.ValueOf(data)
	
	// Dereference pointers
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Map:
		// Handle map
		mapVal := val.MapIndex(reflect.ValueOf(parts[0]))
		if !mapVal.IsValid() {
			return nil
		}
		if len(parts) == 1 {
			return mapVal.Interface()
		}
		return extractNestedValue(mapVal.Interface(), parts[1:])

	case reflect.Struct:
		// Handle struct
		field := val.FieldByName(parts[0])
		if !field.IsValid() {
			// Try case-insensitive match
			for i := 0; i < val.NumField(); i++ {
				fieldName := val.Type().Field(i).Name
				if strings.EqualFold(fieldName, parts[0]) {
					field = val.Field(i)
					break
				}
			}
		}
		if !field.IsValid() {
			return nil
		}
		if len(parts) == 1 {
			return field.Interface()
		}
		return extractNestedValue(field.Interface(), parts[1:])

	default:
		return nil
	}
}

// sendToDLQ sends a failed message to the DLQ
func (c *Consumer) sendToDLQ(
	kafkaMsg *sarama.ConsumerMessage,
	topic Topic,
	errorType string,
	err error,
	retryCount int,
	payload interface{},
) {
	dlqHandler, exists := c.dlqHandlers[kafkaMsg.Topic]
	if !exists || !topic.EnableDLQ() {
		return
	}

	eventID, eventType := "unknown", "unknown"
	var rawMsg map[string]interface{}
	if jsonErr := json.Unmarshal(kafkaMsg.Value, &rawMsg); jsonErr == nil {
		eventID, _ = rawMsg["event_id"].(string)
		eventType, _ = rawMsg["event_type"].(string)
	}

	errorContext := map[string]interface{}{
		"error_type": errorType,
	}

	dlqErr := dlqHandler.SendToDLQ(
		kafkaMsg.Topic,
		topic.DLQTopic(),
		kafkaMsg.Partition,
		kafkaMsg.Offset,
		eventID,
		eventType,
		err.Error(),
		retryCount,
		errorContext,
		payload,
	)

	if dlqErr != nil {
		fmt.Printf("Failed to send message to DLQ: %v\n", dlqErr)
	} else {
		ctx := context.Background()
		c.recordDLQ(ctx, kafkaMsg.Topic)
	}
}

// recordSuccess records successful consumption metrics
func (c *Consumer) recordSuccess(ctx context.Context, topic string, latencyMs float64) {
	if c.config.EnableMetrics && c.messagesConsumed != nil {
		attrs := metric.WithAttributes(attribute.String("topic", topic))
		c.messagesConsumed.Add(ctx, 1, attrs)
		c.handlerLatency.Record(ctx, latencyMs, attrs)
	}
}

// recordError records handler error metrics
func (c *Consumer) recordError(ctx context.Context, topic string) {
	if c.config.EnableMetrics && c.handlerErrors != nil {
		attrs := metric.WithAttributes(attribute.String("topic", topic))
		c.handlerErrors.Add(ctx, 1, attrs)
	}
}

// recordRetry records retry metrics
func (c *Consumer) recordRetry(ctx context.Context, topic string) {
	if c.config.EnableMetrics && c.retries != nil {
		attrs := metric.WithAttributes(attribute.String("topic", topic))
		c.retries.Add(ctx, 1, attrs)
	}
}

// recordDLQ records DLQ metrics
func (c *Consumer) recordDLQ(ctx context.Context, topic string) {
	if c.config.EnableMetrics && c.dlqMessages != nil {
		attrs := metric.WithAttributes(attribute.String("topic", topic))
		c.dlqMessages.Add(ctx, 1, attrs)
	}
}

// Close gracefully shuts down the consumer
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	c.started = false

	// Close consumer group
	if c.consumerGroup != nil {
		if err := c.consumerGroup.Close(); err != nil {
			return err
		}
	}

	// Close DLQ handlers
	for _, dlqHandler := range c.dlqHandlers {
		if err := dlqHandler.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Helper functions

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getMap(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return make(map[string]interface{})
}

