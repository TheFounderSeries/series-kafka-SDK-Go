// Package middleware provides OpenTelemetry metrics functionality
package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsMiddleware provides metrics collection and reporting
type MetricsMiddleware struct {
	meter metric.Meter

	// Producer metrics
	messagesProduced metric.Int64Counter
	produceLatency   metric.Float64Histogram
	produceErrors    metric.Int64Counter
	produceBytes     metric.Int64Counter

	// Consumer metrics
	messagesConsumed metric.Int64Counter
	consumeLatency   metric.Float64Histogram
	consumeErrors    metric.Int64Counter
	consumeBytes     metric.Int64Counter
	consumerLag      metric.Int64Gauge
	retries          metric.Int64Counter
	dlqMessages      metric.Int64Counter
}

// NewMetricsMiddleware creates a new metrics middleware
func NewMetricsMiddleware(serviceName string) (*MetricsMiddleware, error) {
	mm := &MetricsMiddleware{
		meter: otel.Meter(serviceName),
	}

	if err := mm.initProducerMetrics(); err != nil {
		return nil, err
	}

	if err := mm.initConsumerMetrics(); err != nil {
		return nil, err
	}

	return mm, nil
}

// initProducerMetrics initializes producer metrics
func (mm *MetricsMiddleware) initProducerMetrics() error {
	var err error

	mm.messagesProduced, err = mm.meter.Int64Counter(
		"kafka.producer.messages.produced",
		metric.WithDescription("Total number of messages produced"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	mm.produceLatency, err = mm.meter.Float64Histogram(
		"kafka.producer.produce.latency",
		metric.WithDescription("Message production latency in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	mm.produceErrors, err = mm.meter.Int64Counter(
		"kafka.producer.errors",
		metric.WithDescription("Total number of production errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	mm.produceBytes, err = mm.meter.Int64Counter(
		"kafka.producer.bytes",
		metric.WithDescription("Total bytes produced"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return err
	}

	return nil
}

// initConsumerMetrics initializes consumer metrics
func (mm *MetricsMiddleware) initConsumerMetrics() error {
	var err error

	mm.messagesConsumed, err = mm.meter.Int64Counter(
		"kafka.consumer.messages.consumed",
		metric.WithDescription("Total number of messages consumed"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	mm.consumeLatency, err = mm.meter.Float64Histogram(
		"kafka.consumer.handler.latency",
		metric.WithDescription("Message handler latency in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	mm.consumeErrors, err = mm.meter.Int64Counter(
		"kafka.consumer.errors",
		metric.WithDescription("Total number of consumption errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	mm.consumeBytes, err = mm.meter.Int64Counter(
		"kafka.consumer.bytes",
		metric.WithDescription("Total bytes consumed"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return err
	}

	mm.consumerLag, err = mm.meter.Int64Gauge(
		"kafka.consumer.lag",
		metric.WithDescription("Consumer lag (number of messages behind)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	mm.retries, err = mm.meter.Int64Counter(
		"kafka.consumer.retries",
		metric.WithDescription("Total number of message processing retries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	mm.dlqMessages, err = mm.meter.Int64Counter(
		"kafka.consumer.dlq.messages",
		metric.WithDescription("Total number of messages sent to DLQ"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	return nil
}

// RecordProduced records a produced message
func (mm *MetricsMiddleware) RecordProduced(ctx context.Context, topic string, latencyMs float64, bytes int64) {
	attrs := metric.WithAttributes(attribute.String("topic", topic))
	mm.messagesProduced.Add(ctx, 1, attrs)
	mm.produceLatency.Record(ctx, latencyMs, attrs)
	if bytes > 0 {
		mm.produceBytes.Add(ctx, bytes, attrs)
	}
}

// RecordProduceError records a producer error
func (mm *MetricsMiddleware) RecordProduceError(ctx context.Context, topic string, errorType string) {
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("error_type", errorType),
	)
	mm.produceErrors.Add(ctx, 1, attrs)
}

// RecordConsumed records a consumed message
func (mm *MetricsMiddleware) RecordConsumed(ctx context.Context, topic string, latencyMs float64, bytes int64) {
	attrs := metric.WithAttributes(attribute.String("topic", topic))
	mm.messagesConsumed.Add(ctx, 1, attrs)
	mm.consumeLatency.Record(ctx, latencyMs, attrs)
	if bytes > 0 {
		mm.consumeBytes.Add(ctx, bytes, attrs)
	}
}

// RecordConsumeError records a consumer error
func (mm *MetricsMiddleware) RecordConsumeError(ctx context.Context, topic string, errorType string) {
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("error_type", errorType),
	)
	mm.consumeErrors.Add(ctx, 1, attrs)
}

// RecordRetry records a message processing retry
func (mm *MetricsMiddleware) RecordRetry(ctx context.Context, topic string, attempt int) {
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.Int("attempt", attempt),
	)
	mm.retries.Add(ctx, 1, attrs)
}

// RecordDLQ records a message sent to DLQ
func (mm *MetricsMiddleware) RecordDLQ(ctx context.Context, topic string, errorType string) {
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("error_type", errorType),
	)
	mm.dlqMessages.Add(ctx, 1, attrs)
}

// RecordConsumerLag records consumer lag for a topic/partition
func (mm *MetricsMiddleware) RecordConsumerLag(ctx context.Context, topic string, partition int32, lag int64) {
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.Int("partition", int(partition)),
	)
	mm.consumerLag.Record(ctx, lag, attrs)
}

