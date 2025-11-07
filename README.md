# Series Kafka SDK - Go

A production-ready Kafka SDK for Go with full producer/consumer support, DLQ handling, schema validation, OpenTelemetry observability, and topic plugins.

## Features

✅ **Goroutine-Safe** - Producer and consumer are thread-safe  
✅ **Type Safety** - Struct-based validation and type checking  
✅ **Idempotency** - Producer guarantees exactly-once delivery (configurable)  
✅ **DLQ per Topic** - Automatic dead letter queue with standardized error payloads  
✅ **Retry & Backoff** - Exponential backoff with configurable policies  
✅ **OpenTelemetry** - Full tracing and metrics support (optional)  
✅ **Subset Field Extraction** - Performance optimization for consumers  
✅ **Pluggable Topics** - Contract-driven schema system  
✅ **Schema Registry** - Versioning and migration support  

## Installation

```bash
go get github.com/TheFounderSeries/series-kafka-go
```

## Quick Start

### Producer Example

```go
package main

import (
    "context"
    "github.com/TheFounderSeries/series-kafka-go/pkg/core"
    "github.com/TheFounderSeries/series-kafka-go/topics/messages"
)

func main() {
    // Configure producer
    config := &core.ProducerConfig{
        BootstrapServers:  "localhost:9092",
        SASLUsername:      "your-api-key",
        SASLPassword:      "your-api-secret",
        ServiceName:       "my-service",
        EnableIdempotence: true,
        EnableMetrics:     true,
        EnableTracing:     true,
    }

    // Create producer
    producer, err := core.NewProducer(config)
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    // Create topic and payload
    topic := messages.NewMessagesTopic()
    payload := &messages.SendBluePayload{
        AccountEmail:  "test@example.com",
        Content:       "Hello, World!",
        FromNumber:    "+1234567890",
        ToNumber:      "+0987654321",
        MessageHandle: "msg-123",
        Status:        "sent",
        DateSent:      time.Now().Format(time.RFC3339),
        DateUpdated:   time.Now().Format(time.RFC3339),
        IsOutbound:    true,
    }

    // Produce message
    eventID, err := producer.Produce(context.Background(), topic, payload, "message.sendblue")
    if err != nil {
        panic(err)
    }

    fmt.Printf("Message produced: %s\n", eventID)
}
```

### Consumer Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/TheFounderSeries/series-kafka-go/pkg/core"
    "github.com/TheFounderSeries/series-kafka-go/topics/messages"
)

func main() {
    // Configure consumer
    config := &core.ConsumerConfig{
        BootstrapServers: "localhost:9092",
        SASLUsername:     "your-api-key",
        SASLPassword:     "your-api-secret",
        GroupID:          "my-consumer-group",
        ServiceName:      "my-service",
        AutoOffsetReset:  "latest",
        EnableDLQ:        true,
        MaxRetries:       3,
        WorkerCount:      10,
    }

    // Define message handler
    handler := func(ctx context.Context, msg *core.BaseMessage) error {
        fmt.Printf("Received: %s (type: %s)\n", msg.EventID, msg.EventType)
        // Process message...
        return nil
    }

    // Create consumer
    consumer, err := core.NewConsumer(config, []core.Topic{messages.NewMessagesTopic()}, handler)
    if err != nil {
        panic(err)
    }

    // Start consuming
    ctx := context.Background()
    if err := consumer.Start(ctx); err != nil {
        panic(err)
    }

    // Block forever (or until signal)
    select {}
}
```

### Subset Field Extraction (Performance Optimization)

```go
config := &core.ConsumerConfig{
    // ...
    SubsetFields: []string{"payload.user_id", "payload.content", "event_id"},
}

subsetHandler := func(ctx context.Context, fields map[string]interface{}) error {
    userID := fields["user_id"].(string)
    content := fields["content"].(string)
    // Only these fields are extracted, improving performance
    return nil
}

consumer, _ := core.NewConsumerWithSubsetHandler(config, topics, subsetHandler)
```

## Configuration

### Environment Variables

```bash
KAFKA_BOOTSTRAP_SERVERS=pkc-xxx.us-east4.gcp.confluent.cloud:9092
KAFKA_API_KEY=your-api-key
KAFKA_API_SECRET=your-api-secret
SERVICE_NAME=my-service
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

Load from environment:

```go
producerConfig, err := core.LoadProducerConfigFromEnv()
consumerConfig, err := core.LoadConsumerConfigFromEnv("my-consumer-group")
```

## Testing

```bash
# Run unit tests
go test ./tests/unit/...

# Run with race detector
go test -race ./tests/unit/...

# Run integration tests (requires local Kafka)
docker-compose up -d
go test -v ./tests/integration/...

# Clean up
docker-compose down
```

## Architecture

```
pkg/
├── core/          # Producer, consumer, message, DLQ
├── schema/        # Serialization, validation, registry
├── contracts/     # Versioning, migration
├── middleware/    # OpenTelemetry tracing & metrics
└── errors/        # Custom error types

topics/
└── messages/      # Example topic plugin
```

## Creating Custom Topics

```go
type MyTopic struct {
    *core.BaseTopic
}

func NewMyTopic() *MyTopic {
    schemaRegistry := map[string]interface{}{
        "my.event": &MyEventPayload{},
    }
    return &MyTopic{
        BaseTopic: core.NewBaseTopic("my-topic", schemaRegistry),
    }
}

func (t *MyTopic) GetPartitionKey(msg *core.BaseMessage, eventType string) string {
    // Custom partitioning logic
    return "partition-key"
}

func (t *MyTopic) ValidateMessage(msg *core.BaseMessage) error {
    // Custom validation
    return msg.Validate()
}
```

## License

MIT

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

