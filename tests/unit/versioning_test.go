package unit

import (
	"testing"

	"github.com/TheFounderSeries/series-kafka-go/pkg/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVersion(t *testing.T) {
	tests := []struct {
		name        string
		versionStr  string
		expectError bool
		expected    *contracts.Version
	}{
		{
			name:        "valid version",
			versionStr:  "1.2.3",
			expectError: false,
			expected:    &contracts.Version{Major: 1, Minor: 2, Patch: 3},
		},
		{
			name:        "zero version",
			versionStr:  "0.0.0",
			expectError: false,
			expected:    &contracts.Version{Major: 0, Minor: 0, Patch: 0},
		},
		{
			name:        "invalid format - too few parts",
			versionStr:  "1.2",
			expectError: true,
		},
		{
			name:        "invalid format - too many parts",
			versionStr:  "1.2.3.4",
			expectError: true,
		},
		{
			name:        "invalid format - non-numeric",
			versionStr:  "1.2.a",
			expectError: true,
		},
		{
			name:        "negative version",
			versionStr:  "1.-2.3",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, err := contracts.ParseVersion(tt.versionStr)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected.Major, version.Major)
				assert.Equal(t, tt.expected.Minor, version.Minor)
				assert.Equal(t, tt.expected.Patch, version.Patch)
			}
		})
	}
}

func TestVersion_Compare(t *testing.T) {
	v1, _ := contracts.ParseVersion("1.2.3")
	v2, _ := contracts.ParseVersion("1.2.4")
	v3, _ := contracts.ParseVersion("1.3.0")
	v4, _ := contracts.ParseVersion("2.0.0")
	v5, _ := contracts.ParseVersion("1.2.3")

	assert.Equal(t, -1, v1.Compare(v2)) // 1.2.3 < 1.2.4
	assert.Equal(t, -1, v1.Compare(v3)) // 1.2.3 < 1.3.0
	assert.Equal(t, -1, v1.Compare(v4)) // 1.2.3 < 2.0.0
	assert.Equal(t, 0, v1.Compare(v5))  // 1.2.3 == 1.2.3
	assert.Equal(t, 1, v2.Compare(v1))  // 1.2.4 > 1.2.3
}

func TestVersion_IsCompatibleWith(t *testing.T) {
	v1, _ := contracts.ParseVersion("1.2.3")
	v2, _ := contracts.ParseVersion("1.5.0")
	v3, _ := contracts.ParseVersion("2.0.0")

	assert.True(t, v1.IsCompatibleWith(v2))   // Same major version
	assert.False(t, v1.IsCompatibleWith(v3))  // Different major version
}

func TestVersion_Increment(t *testing.T) {
	v, _ := contracts.ParseVersion("1.2.3")

	major := v.IncrementMajor()
	assert.Equal(t, "2.0.0", major.String())

	minor := v.IncrementMinor()
	assert.Equal(t, "1.3.0", minor.String())

	patch := v.IncrementPatch()
	assert.Equal(t, "1.2.4", patch.String())
}

func TestIsBreakingChange(t *testing.T) {
	v1, _ := contracts.ParseVersion("1.2.3")
	v2, _ := contracts.ParseVersion("1.3.0")
	v3, _ := contracts.ParseVersion("2.0.0")

	assert.False(t, contracts.IsBreakingChange(v1, v2)) // Minor version bump
	assert.True(t, contracts.IsBreakingChange(v1, v3))  // Major version bump
}

