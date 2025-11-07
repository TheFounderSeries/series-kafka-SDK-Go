// Package core provides configuration types
package core

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// ProducerConfig holds configuration for the Kafka producer
type ProducerConfig struct {
	// Kafka connection settings
	BootstrapServers string
	SASLUsername     string
	SASLPassword     string

	// Service identity
	ServiceName string

	// Producer settings
	EnableIdempotence bool // Default: true for exactly-once delivery
	MaxRetries        int  // Default: 3
	RetryBackoffMs    int  // Default: 100ms

	// Observability
	EnableMetrics bool // Default: true
	EnableTracing bool // Default: true

	// Schema registry (optional)
	SchemaRegistryURL string

	// Advanced Sarama configuration (optional overrides)
	SaramaConfig *sarama.Config
}

// ConsumerConfig holds configuration for the Kafka consumer
type ConsumerConfig struct {
	// Kafka connection settings
	BootstrapServers string
	SASLUsername     string
	SASLPassword     string

	// Consumer group settings
	GroupID     string
	ServiceName string

	// Consumer behavior
	AutoOffsetReset string // "earliest" or "latest" (default: "latest")
	MaxPollRecords  int    // Default: 500

	// DLQ settings
	EnableDLQ      bool // Default: true
	MaxRetries     int  // Default: 3
	RetryBackoffMs int  // Default: 100ms (exponential)

	// Worker pool
	WorkerCount int // Number of goroutines for concurrent processing (default: 10)

	// Event filtering
	EventTypeFilter []string // Only consume specific event types (empty = all)

	// Subset field extraction (dot-notation paths)
	SubsetFields []string // e.g., ["payload.user_id", "payload.username", "event_id"]

	// Observability
	EnableMetrics bool // Default: true
	EnableTracing bool // Default: true

	// Schema registry (optional)
	SchemaRegistryURL string

	// Advanced Sarama configuration (optional overrides)
	SaramaConfig *sarama.Config
}

// LoadProducerConfigFromEnv loads producer configuration from environment variables
func LoadProducerConfigFromEnv() (*ProducerConfig, error) {
	config := &ProducerConfig{
		BootstrapServers:  getEnv("KAFKA_BOOTSTRAP_SERVERS", ""),
		SASLUsername:      getEnv("KAFKA_API_KEY", ""),
		SASLPassword:      getEnv("KAFKA_API_SECRET", ""),
		ServiceName:       getEnv("SERVICE_NAME", ""),
		EnableIdempotence: getEnvBool("KAFKA_ENABLE_IDEMPOTENCE", true),
		MaxRetries:        getEnvInt("KAFKA_MAX_RETRIES", 3),
		RetryBackoffMs:    getEnvInt("KAFKA_RETRY_BACKOFF_MS", 100),
		EnableMetrics:     getEnvBool("OTEL_ENABLE_METRICS", true),
		EnableTracing:     getEnvBool("OTEL_ENABLE_TRACING", true),
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", ""),
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// LoadConsumerConfigFromEnv loads consumer configuration from environment variables
func LoadConsumerConfigFromEnv(groupID string) (*ConsumerConfig, error) {
	config := &ConsumerConfig{
		BootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", ""),
		SASLUsername:     getEnv("KAFKA_API_KEY", ""),
		SASLPassword:     getEnv("KAFKA_API_SECRET", ""),
		GroupID:          groupID,
		ServiceName:      getEnv("SERVICE_NAME", ""),
		AutoOffsetReset:  getEnv("KAFKA_AUTO_OFFSET_RESET", "latest"),
		MaxPollRecords:   getEnvInt("KAFKA_MAX_POLL_RECORDS", 500),
		EnableDLQ:        getEnvBool("KAFKA_ENABLE_DLQ", true),
		MaxRetries:       getEnvInt("KAFKA_MAX_RETRIES", 3),
		RetryBackoffMs:   getEnvInt("KAFKA_RETRY_BACKOFF_MS", 100),
		WorkerCount:      getEnvInt("KAFKA_WORKER_COUNT", 10),
		EnableMetrics:    getEnvBool("OTEL_ENABLE_METRICS", true),
		EnableTracing:    getEnvBool("OTEL_ENABLE_TRACING", true),
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", ""),
	}

	// Parse event type filter if provided
	if filter := getEnv("KAFKA_EVENT_TYPE_FILTER", ""); filter != "" {
		config.EventTypeFilter = strings.Split(filter, ",")
		for i, et := range config.EventTypeFilter {
			config.EventTypeFilter[i] = strings.TrimSpace(et)
		}
	}

	// Parse subset fields if provided
	if fields := getEnv("KAFKA_SUBSET_FIELDS", ""); fields != "" {
		config.SubsetFields = strings.Split(fields, ",")
		for i, field := range config.SubsetFields {
			config.SubsetFields[i] = strings.TrimSpace(field)
		}
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// Validate validates the producer configuration
func (c *ProducerConfig) Validate() error {
	if c.BootstrapServers == "" {
		return sdkerrors.NewValidationError("bootstrap_servers", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.SASLUsername == "" {
		return sdkerrors.NewValidationError("sasl_username", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.SASLPassword == "" {
		return sdkerrors.NewValidationError("sasl_password", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.ServiceName == "" {
		return sdkerrors.NewValidationError("service_name", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.MaxRetries < 0 {
		return sdkerrors.NewValidationError("max_retries", "must be >= 0", sdkerrors.ErrInvalidConfig)
	}
	if c.RetryBackoffMs < 0 {
		return sdkerrors.NewValidationError("retry_backoff_ms", "must be >= 0", sdkerrors.ErrInvalidConfig)
	}
	return nil
}

// Validate validates the consumer configuration
func (c *ConsumerConfig) Validate() error {
	if c.BootstrapServers == "" {
		return sdkerrors.NewValidationError("bootstrap_servers", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.SASLUsername == "" {
		return sdkerrors.NewValidationError("sasl_username", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.SASLPassword == "" {
		return sdkerrors.NewValidationError("sasl_password", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.GroupID == "" {
		return sdkerrors.NewValidationError("group_id", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.ServiceName == "" {
		return sdkerrors.NewValidationError("service_name", "cannot be empty", sdkerrors.ErrInvalidConfig)
	}
	if c.AutoOffsetReset != "earliest" && c.AutoOffsetReset != "latest" {
		return sdkerrors.NewValidationError("auto_offset_reset", "must be 'earliest' or 'latest'", sdkerrors.ErrInvalidConfig)
	}
	if c.MaxRetries < 0 {
		return sdkerrors.NewValidationError("max_retries", "must be >= 0", sdkerrors.ErrInvalidConfig)
	}
	if c.WorkerCount < 1 {
		return sdkerrors.NewValidationError("worker_count", "must be >= 1", sdkerrors.ErrInvalidConfig)
	}
	return nil
}

// BuildSaramaProducerConfig creates a Sarama producer config from SDK config
func (c *ProducerConfig) BuildSaramaProducerConfig() (*sarama.Config, error) {
	if c.SaramaConfig != nil {
		return c.SaramaConfig, nil
	}

	config := sarama.NewConfig()

	// Producer settings
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas

	// Idempotency
	if c.EnableIdempotence {
		config.Producer.Idempotent = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Net.MaxOpenRequests = 1 // Required for idempotency
	}

	// Retry settings
	config.Producer.Retry.Max = c.MaxRetries
	config.Producer.Retry.Backoff = time.Duration(c.RetryBackoffMs) * time.Millisecond

	// Compression
	config.Producer.Compression = sarama.CompressionSnappy

	// SASL authentication
	config.Net.SASL.Enable = true
	config.Net.SASL.User = c.SASLUsername
	config.Net.SASL.Password = c.SASLPassword
	config.Net.SASL.Handshake = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	// TLS
	config.Net.TLS.Enable = true

	// Set version (required for idempotency)
	config.Version = sarama.V2_5_0_0

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid sarama producer config: %w", err)
	}

	return config, nil
}

// BuildSaramaConsumerConfig creates a Sarama consumer config from SDK config
func (c *ConsumerConfig) BuildSaramaConsumerConfig() (*sarama.Config, error) {
	if c.SaramaConfig != nil {
		return c.SaramaConfig, nil
	}

	config := sarama.NewConfig()

	// Consumer settings
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// Auto offset reset
	if c.AutoOffsetReset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Max processing time
	config.Consumer.MaxProcessingTime = 30 * time.Second

	// SASL authentication (only if credentials provided)
	if c.SASLUsername != "" && c.SASLPassword != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SASLUsername
		config.Net.SASL.Password = c.SASLPassword
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

		// TLS (only with SASL)
		config.Net.TLS.Enable = true
	}

	// Set version
	config.Version = sarama.V2_5_0_0

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid sarama consumer config: %w", err)
	}

	return config, nil
}

// Helper functions for environment variable parsing

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return defaultValue
}

