// Package core provides Kafka producer implementation
package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Producer provides a goroutine-safe Kafka producer with schema validation,
// idempotent delivery, and OpenTelemetry observability.
type Producer struct {
	config   *ProducerConfig
	producer sarama.SyncProducer
	mu       sync.RWMutex
	started  bool

	// OpenTelemetry
	tracer            trace.Tracer
	meter             metric.Meter
	messagesProduced  metric.Int64Counter
	produceLatency    metric.Float64Histogram
	produceErrors     metric.Int64Counter
}

// NewProducer creates a new Kafka producer
func NewProducer(config *ProducerConfig) (*Producer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Build Sarama config
	saramaConfig, err := config.BuildSaramaProducerConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build sarama config: %w", err)
	}

	// Create producer
	brokers := parseBrokers(config.BootstrapServers)
	producer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		return nil, sdkerrors.NewProducerError("init", "", "failed to create producer", err)
	}

	p := &Producer{
		config:   config,
		producer: producer,
		started:  true,
	}

	// Initialize OpenTelemetry if enabled
	if config.EnableTracing {
		p.tracer = otel.Tracer("series-kafka-producer")
	}

	if config.EnableMetrics {
		p.meter = otel.Meter("series-kafka-producer")
		if err := p.setupMetrics(); err != nil {
			// Metrics setup failure is not fatal, just log and continue
			fmt.Printf("Warning: failed to setup metrics: %v\n", err)
		}
	}

	return p, nil
}

// setupMetrics initializes OpenTelemetry metrics
func (p *Producer) setupMetrics() error {
	var err error

	p.messagesProduced, err = p.meter.Int64Counter(
		"kafka.producer.messages.produced",
		metric.WithDescription("Total number of messages produced"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	p.produceLatency, err = p.meter.Float64Histogram(
		"kafka.producer.produce.latency",
		metric.WithDescription("Message production latency"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	p.produceErrors, err = p.meter.Int64Counter(
		"kafka.producer.errors",
		metric.WithDescription("Total number of production errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	return nil
}

// Produce produces a message to a topic with full validation and observability
func (p *Producer) Produce(
	ctx context.Context,
	topic Topic,
	payload interface{},
	eventType string,
	opts ...ProduceOption,
) (string, error) {
	p.mu.RLock()
	if !p.started {
		p.mu.RUnlock()
		return "", sdkerrors.ErrProducerNotStarted
	}
	p.mu.RUnlock()

	startTime := time.Now()

	// Start tracing span if enabled
	var span trace.Span
	if p.config.EnableTracing && p.tracer != nil {
		ctx, span = p.tracer.Start(ctx, "kafka.produce",
			trace.WithAttributes(
				attribute.String("kafka.topic", topic.Name()),
				attribute.String("kafka.event_type", eventType),
			),
		)
		defer span.End()
	}

	// 1. Validate event type is registered in topic schema
	schemaRegistry := topic.SchemaRegistry()
	if _, exists := schemaRegistry[eventType]; !exists {
		err := sdkerrors.NewValidationError("event_type", 
			fmt.Sprintf("event type '%s' not registered in topic '%s'", eventType, topic.Name()),
			sdkerrors.ErrSchemaNotFound)
		p.recordError(ctx, topic.Name())
		return "", err
	}

	// 2. Create base message
	msg, err := NewBaseMessage(eventType, p.config.ServiceName, payload)
	if err != nil {
		p.recordError(ctx, topic.Name())
		return "", err
	}

	// 3. Apply options
	for _, opt := range opts {
		opt(msg)
	}

	// 4. Validate message
	if err := topic.ValidateMessage(msg); err != nil {
		p.recordError(ctx, topic.Name())
		return "", sdkerrors.NewValidationError("message", "topic validation failed", err)
	}

	// 5. Add trace context to message if available
	if span != nil {
		spanCtx := span.SpanContext()
		if spanCtx.HasTraceID() {
			traceID := spanCtx.TraceID().String()
			msg.WithTraceID(traceID)
			spanID := spanCtx.SpanID().String()
			msg.WithParentSpanID(spanID)
		}
	}

	// 6. Get partition key
	partitionKey := topic.GetPartitionKey(msg, eventType)

	// 7. Serialize message
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		p.recordError(ctx, topic.Name())
		return "", sdkerrors.NewProducerError("serialize", topic.Name(), "failed to serialize message", err)
	}

	// 8. Create Kafka message
	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic.Name(),
		Value: sarama.ByteEncoder(msgBytes),
	}

	if partitionKey != "" {
		kafkaMsg.Key = sarama.StringEncoder(partitionKey)
	}

	// 9. Send message (synchronously for guaranteed delivery)
	partition, offset, err := p.producer.SendMessage(kafkaMsg)
	if err != nil {
		p.recordError(ctx, topic.Name())
		return "", sdkerrors.NewProducerError("send", topic.Name(), "failed to send message", err)
	}

	// 10. Record metrics
	latencyMs := float64(time.Since(startTime).Milliseconds())
	p.recordSuccess(ctx, topic.Name(), latencyMs)

	// Add span attributes for successful send
	if span != nil {
		span.SetAttributes(
			attribute.Int64("kafka.partition", int64(partition)),
			attribute.Int64("kafka.offset", offset),
			attribute.String("kafka.event_id", msg.EventID),
		)
	}

	return msg.EventID, nil
}

// recordSuccess records successful produce metrics
func (p *Producer) recordSuccess(ctx context.Context, topic string, latencyMs float64) {
	if p.config.EnableMetrics && p.messagesProduced != nil {
		attrs := metric.WithAttributes(attribute.String("topic", topic))
		p.messagesProduced.Add(ctx, 1, attrs)
		p.produceLatency.Record(ctx, latencyMs, attrs)
	}
}

// recordError records produce error metrics
func (p *Producer) recordError(ctx context.Context, topic string) {
	if p.config.EnableMetrics && p.produceErrors != nil {
		attrs := metric.WithAttributes(attribute.String("topic", topic))
		p.produceErrors.Add(ctx, 1, attrs)
	}
}

// Close gracefully shuts down the producer
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return nil
	}

	p.started = false
	return p.producer.Close()
}

// ProduceOption is a functional option for customizing message production
type ProduceOption func(*BaseMessage)

// WithCorrelationID sets the correlation ID for the message
func WithCorrelationID(correlationID string) ProduceOption {
	return func(msg *BaseMessage) {
		msg.WithCorrelationID(correlationID)
	}
}

// WithSchemaVersion sets a custom schema version
func WithSchemaVersion(version string) ProduceOption {
	return func(msg *BaseMessage) {
		_ = msg.SetSchemaVersion(version) // Ignore error - validation happens later
	}
}

// WithMetadata adds metadata to the message
func WithMetadata(key string, value interface{}) ProduceOption {
	return func(msg *BaseMessage) {
		msg.WithMetadata(key, value)
	}
}

// WithTracing sets trace and span IDs manually
func WithTracing(traceID, parentSpanID string) ProduceOption {
	return func(msg *BaseMessage) {
		msg.WithTraceID(traceID)
		msg.WithParentSpanID(parentSpanID)
	}
}

