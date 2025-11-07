package unit

import (
	"os"
	"testing"

	"github.com/TheFounderSeries/series-kafka-go/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *core.ProducerConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: &core.ProducerConfig{
				BootstrapServers:  "localhost:9092",
				SASLUsername:      "user",
				SASLPassword:      "pass",
				ServiceName:       "test-service",
				EnableIdempotence: true,
				MaxRetries:        3,
				RetryBackoffMs:    100,
			},
			expectError: false,
		},
		{
			name: "missing bootstrap servers",
			config: &core.ProducerConfig{
				BootstrapServers:  "",
				SASLUsername:      "user",
				SASLPassword:      "pass",
				ServiceName:       "test-service",
			},
			expectError: true,
		},
		{
			name: "missing service name",
			config: &core.ProducerConfig{
				BootstrapServers:  "localhost:9092",
				SASLUsername:      "user",
				SASLPassword:      "pass",
				ServiceName:       "",
			},
			expectError: true,
		},
		{
			name: "negative max retries",
			config: &core.ProducerConfig{
				BootstrapServers:  "localhost:9092",
				SASLUsername:      "user",
				SASLPassword:      "pass",
				ServiceName:       "test-service",
				MaxRetries:        -1,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConsumerConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *core.ConsumerConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: &core.ConsumerConfig{
				BootstrapServers: "localhost:9092",
				SASLUsername:     "user",
				SASLPassword:     "pass",
				GroupID:          "test-group",
				ServiceName:      "test-service",
				AutoOffsetReset:  "latest",
				MaxRetries:       3,
				WorkerCount:      10,
			},
			expectError: false,
		},
		{
			name: "invalid auto offset reset",
			config: &core.ConsumerConfig{
				BootstrapServers: "localhost:9092",
				SASLUsername:     "user",
				SASLPassword:     "pass",
				GroupID:          "test-group",
				ServiceName:      "test-service",
				AutoOffsetReset:  "invalid",
			},
			expectError: true,
		},
		{
			name: "zero worker count",
			config: &core.ConsumerConfig{
				BootstrapServers: "localhost:9092",
				SASLUsername:     "user",
				SASLPassword:     "pass",
				GroupID:          "test-group",
				ServiceName:      "test-service",
				AutoOffsetReset:  "latest",
				WorkerCount:      0,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadProducerConfigFromEnv(t *testing.T) {
	// Set environment variables
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	os.Setenv("KAFKA_API_KEY", "test-key")
	os.Setenv("KAFKA_API_SECRET", "test-secret")
	os.Setenv("SERVICE_NAME", "test-service")
	defer func() {
		os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")
		os.Unsetenv("KAFKA_API_KEY")
		os.Unsetenv("KAFKA_API_SECRET")
		os.Unsetenv("SERVICE_NAME")
	}()

	config, err := core.LoadProducerConfigFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "localhost:9092", config.BootstrapServers)
	assert.Equal(t, "test-key", config.SASLUsername)
	assert.Equal(t, "test-secret", config.SASLPassword)
	assert.Equal(t, "test-service", config.ServiceName)
	assert.True(t, config.EnableIdempotence)
}

func TestBuildSaramaProducerConfig(t *testing.T) {
	config := &core.ProducerConfig{
		BootstrapServers:  "localhost:9092",
		SASLUsername:      "user",
		SASLPassword:      "pass",
		ServiceName:       "test-service",
		EnableIdempotence: true,
		MaxRetries:        3,
		RetryBackoffMs:    100,
	}

	saramaConfig, err := config.BuildSaramaProducerConfig()
	require.NoError(t, err)
	assert.NotNil(t, saramaConfig)
	assert.True(t, saramaConfig.Producer.Idempotent)
	assert.Equal(t, 3, saramaConfig.Producer.Retry.Max)
}

