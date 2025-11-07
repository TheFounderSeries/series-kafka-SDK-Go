// Package middleware provides OpenTelemetry tracing functionality
package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TracingMiddleware provides distributed tracing support
type TracingMiddleware struct {
	tracer trace.Tracer
}

// NewTracingMiddleware creates a new tracing middleware
func NewTracingMiddleware(serviceName string) *TracingMiddleware {
	return &TracingMiddleware{
		tracer: otel.Tracer(serviceName),
	}
}

// StartProducerSpan starts a new span for message production
func (tm *TracingMiddleware) StartProducerSpan(
	ctx context.Context,
	topicName string,
	eventType string,
	eventID string,
) (context.Context, trace.Span) {
	ctx, span := tm.tracer.Start(ctx, "kafka.produce",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topicName),
			attribute.String("messaging.operation", "publish"),
			attribute.String("kafka.event_type", eventType),
			attribute.String("kafka.event_id", eventID),
		),
	)
	return ctx, span
}

// StartConsumerSpan starts a new span for message consumption
func (tm *TracingMiddleware) StartConsumerSpan(
	ctx context.Context,
	topicName string,
	partition int32,
	offset int64,
	eventType string,
	eventID string,
) (context.Context, trace.Span) {
	ctx, span := tm.tracer.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source", topicName),
			attribute.String("messaging.operation", "receive"),
			attribute.Int64("kafka.partition", int64(partition)),
			attribute.Int64("kafka.offset", offset),
			attribute.String("kafka.event_type", eventType),
			attribute.String("kafka.event_id", eventID),
		),
	)
	return ctx, span
}

// AddProducerAttributes adds attributes to a producer span
func (tm *TracingMiddleware) AddProducerAttributes(span trace.Span, attrs ...attribute.KeyValue) {
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// AddConsumerAttributes adds attributes to a consumer span
func (tm *TracingMiddleware) AddConsumerAttributes(span trace.Span, attrs ...attribute.KeyValue) {
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// RecordError records an error in the current span
func (tm *TracingMiddleware) RecordError(span trace.Span, err error, description string) {
	if span.IsRecording() {
		span.RecordError(err,
			trace.WithAttributes(
				attribute.String("error.description", description),
			),
		)
	}
}

// ExtractTraceContext extracts trace context from a message
// Returns traceID and spanID if present
func ExtractTraceContext(traceID, parentSpanID *string) (trace.SpanContext, bool) {
	if traceID == nil || parentSpanID == nil {
		return trace.SpanContext{}, false
	}

	// Parse trace ID
	tid, err := trace.TraceIDFromHex(*traceID)
	if err != nil {
		return trace.SpanContext{}, false
	}

	// Parse span ID
	sid, err := trace.SpanIDFromHex(*parentSpanID)
	if err != nil {
		return trace.SpanContext{}, false
	}

	// Create span context
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid,
		SpanID:  sid,
		TraceFlags: trace.FlagsSampled,
	})

	return spanCtx, spanCtx.IsValid()
}

// InjectTraceContext injects trace context into a message
// Returns traceID and spanID strings
func InjectTraceContext(span trace.Span) (traceID string, spanID string) {
	if !span.IsRecording() {
		return "", ""
	}

	spanCtx := span.SpanContext()
	if !spanCtx.IsValid() {
		return "", ""
	}

	return spanCtx.TraceID().String(), spanCtx.SpanID().String()
}

