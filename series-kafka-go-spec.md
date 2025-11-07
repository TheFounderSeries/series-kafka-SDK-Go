Series Kafka SDK - Go Technical Specification
Version: 1.0.0
Language: Go 1.21+
Kafka Library: confluent-kafka-go / Sarama
Last Updated: November 7, 2025

Table of Contents
Executive Summary

Go-Specific Architecture

Installation & Setup

Core Components

Producer Implementation

Consumer Implementation (NEW, now fully supported)

Topic Plugins

Schema Management

Error Handling & DLQ

Observability

Service Integration Examples

Testing

Best Practices

API Reference

1. Executive Summary
1.1 Purpose
The Series Kafka Go SDK provides high-performance, type-safe event streaming for Go and microservices on the Series platform. It now supports full producer and consumer logic, including all business and logical principles as in the Python SDK (such as DLQ, async-safe consumer, contract-driven schemas, schema registry, idempotency, partitioning, OpenTelemetry, and code-generation-ready topic plugins).

1.2 Target Services (Go)
Producer and Consumer Services

SendBlue Service (Messages topic)

Linq Service (Messages topic)

Any new Go-based microservice producing/consuming Series Kafka events

1.3 Key Features
✅ Goroutine/Channel-Based - Producer AND consumer concurrency

✅ Type Safety - Struct and interface-based validation

✅ Idempotency - Producer guarantees at-least and exactly-once delivery

✅ DLQ per Topic and Consumer - Auto-generated, standardized error payload

✅ Retry & Backoff - Policy-driven, with exponential strategy

✅ OpenTelemetry - Metrics, tracing for both producer and consumer

✅ Pluggable Topic/Schema Contract System

✅ Partition/Key logic, Subset Field Support on consumption

✅ Full Contract Versioning and Schema Registry

✅ Error handling and recovery

2. Go-Specific Architecture
2.1 Package Structure
text
series-kafka-go/
├── go.mod
├── go.sum
├── README.md
├── pkg/
│   ├── core/
│   │   ├── producer.go
│   │   ├── consumer.go         # <--- New: Async/parallel consumer
│   │   ├── message.go
│   │   ├── topic.go
│   │   ├── config.go
│   │   ├── errors.go
│   │   └── dlq.go              # <--- New: DLQ
│   ├── schema/
│   │   ├── registry.go
│   │   ├── validator.go
│   │   └── serializer.go
│   ├── contracts/
│   │   ├── versioning.go
│   │   ├── contract.go
│   │   └── migration.go
│   ├── middleware/
│   │   ├── tracing.go
│   │   └── metrics.go
│   └── errors/
│       └── errors.go
├── topics/
│   └── messages/
│       ├── topic.go
│       ├── schemas.go
│       └── contract.go
├── tests/
└── examples/
2.2 Dependencies
As before, plus anything needed for consumer concurrency and OpenTelemetry.

2.3 Concurrency & Streaming
Producers (Produce) are goroutine-safe, built for high-throughput streaming.

Consumers use goroutines/channels: consumption is non-blocking, scalable, and works seamlessly with Go's concurrency model.

Consumer can spawn multiple workers (goroutine pools) for parallel message processing.

Delivery and error reports are reported via channels and error interfaces, to enable streaming APIs everywhere.

3. Installation & Setup
(Same as before, add install/go-get for confluent-kafka-go, otel, etc.)

4. Core Components
4.1 Message Envelope
Equivalent struct to match Python's BaseMessage.
All contract, field naming, and versioning/metadata matches Python.

go
type BaseMessage struct {
    EventID       string                 `json:"event_id"`
    EventType     string                 `json:"event_type"` // e.g. "user.created"
    SchemaVersion string                 `json:"schema_version"`
    Timestamp     time.Time              `json:"timestamp"`
    SourceService string                 `json:"source_service"`
    Payload       interface{}            `json:"payload"`
    CorrelationID *string                `json:"correlation_id,omitempty"`
    TraceID       *string                `json:"trace_id,omitempty"`
    ParentSpanID  *string                `json:"parent_span_id,omitempty"`
    Metadata      map[string]interface{} `json:"metadata,omitempty"`
}
5. Producer Implementation
Goroutine-safe.

Schema/contract validation before produce (uses struct tags or external registry).

Partition key extraction per topic logic.

Idempotent delivery by default.

OpenTelemetry integration.

Pluggable serialization.

6. Consumer Implementation (NEW, FULLY SUPPORTED)
6.1 API & Features
Safe for concurrent consumption from multiple partitions/topics.

DLQ auto-generation and standardized error payload.

Retry/back-off logic user configurable per topic.

OpenTelemetry integration (message rates, handler latency, errors, lag).

Partition key and subset field support: can project just a subset of payload fields for business filtering.

Message handler is a user-passed function (streaming or batch APIs).

Contract validation/compatibility before handler invocation.

Delivery reports and errors returned via channels and error hooks.

6.2 Consumer Example
go
import (
    "context"
    "github.com/series/kafka-sdk-go/pkg/core"
    "github.com/series/kafka-sdk-go/topics/messages"
)

// Handler function called for every incoming message.
func handleMessage(ctx context.Context, msg *core.BaseMessage) error {
    // Decode payload here (using type assertion or generic unmarshal)
    payload := msg.Payload.(*messages.SendBluePayload)
    // Business logic...
    return nil
}

// Consumer usage example
func main() {
    config := &core.ConsumerConfig{
        BootstrapServers: "...",
        SASLUsername:     "...",
        SASLPassword:     "...",
        GroupID:          "sendblue-messages",
        ServiceName:      "sendblue",
        // DLQ, retry, metrics, etc.
    }
    topic := messages.NewMessagesTopic()
    consumer, err := core.NewConsumer(config, []core.Topic{topic}, handleMessage)
    if err != nil {
        panic(err)
    }

    // Start consumption in background goroutines (handles concurrency)
    go consumer.Start(context.Background())

    // Shutdown gracefully on sigterm, etc.
    // ...
}
Features:

Consumer can spawn N goroutine workers (configurable) to process messages concurrently.

Message acknowledgment and offset committing are automatic, with manual options for advanced workflows.

Subset field support (e.g. only consume fields ["payload.user_id", "payload.content"])

All error and business logic patterns mirror the Python SDK.

7. Topic Plugins
All topic plugin pattern/logic/contract matches Python (see Python spec for naming, event type/partition conventions, validation, etc.).

8. Schema Management
Supports Go struct-based validation (tags), external JSON Schema registry integration for contract validation/versioning, and code generation.

Consumers and producers validate using the same patterns as Python.

9. Error Handling & DLQ
DLQ topics auto-generated ({topic}.dlq).

DLQ message schema matches Python SDK for error payloads (event_id, event_type, ts, error desc, context, payload opt-in).

Retry/backoff configurable per handler; automatic DLQ on max retries.

10. Observability
OpenTelemetry tracing and metrics on produce/consume (full parity with Python).

Metrics: message rates, handler latency, retries, DLQ rates, lag, partition assignment, commit latency, etc.

11. Service Integration Examples
See "Consumer Example" above for Go pattern.

Producer/consumer logic is nearly identical to Python SDK, all concurrency handled by goroutines/channels/open worker pools.

Example covers both HTTP webhook services AND background message-driven services.

12. Testing
Table-driven unit tests, testable handler injection.

Integration tests for real Kafka clusters (docker-compose, testcontainers-go).

Contract-based tests: check compatibility with Python schemas/contracts.

13. Best Practices
Use context.Context, defer cleanup, always check returned errors.

Only one producer/consumer per app, reused as needed (thread-safe).

Configure DLQ and retry for all critical topics, never drop unhandled messages.

Validate all contracts/schemas; update client code with generated codegen when contracts change.