// Package contracts provides version management
package contracts

import (
	"fmt"
	"strconv"
	"strings"

	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// Version represents a semantic version
type Version struct {
	Major int
	Minor int
	Patch int
}

// ParseVersion parses a semantic version string (e.g., "1.2.3")
func ParseVersion(versionStr string) (*Version, error) {
	parts := strings.Split(versionStr, ".")
	if len(parts) != 3 {
		return nil, sdkerrors.NewValidationError("version",
			"must be in format 'major.minor.patch' (e.g., '1.0.0')",
			nil)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, sdkerrors.NewValidationError("version.major",
			"must be a valid integer",
			err)
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, sdkerrors.NewValidationError("version.minor",
			"must be a valid integer",
			err)
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, sdkerrors.NewValidationError("version.patch",
			"must be a valid integer",
			err)
	}

	if major < 0 || minor < 0 || patch < 0 {
		return nil, sdkerrors.NewValidationError("version",
			"major, minor, and patch must be non-negative",
			nil)
	}

	return &Version{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, nil
}

// String returns the string representation of the version
func (v *Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Compare compares two versions
// Returns:
//   -1 if v < other
//    0 if v == other
//    1 if v > other
func (v *Version) Compare(other *Version) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}
		return 1
	}

	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	}

	if v.Patch != other.Patch {
		if v.Patch < other.Patch {
			return -1
		}
		return 1
	}

	return 0
}

// IsCompatibleWith checks if this version is compatible with another version
// Compatible if major versions match (following semantic versioning)
func (v *Version) IsCompatibleWith(other *Version) bool {
	return v.Major == other.Major
}

// IsGreaterThan checks if this version is greater than another
func (v *Version) IsGreaterThan(other *Version) bool {
	return v.Compare(other) > 0
}

// IsLessThan checks if this version is less than another
func (v *Version) IsLessThan(other *Version) bool {
	return v.Compare(other) < 0
}

// Equals checks if this version equals another
func (v *Version) Equals(other *Version) bool {
	return v.Compare(other) == 0
}

// IncrementMajor creates a new version with incremented major version
func (v *Version) IncrementMajor() *Version {
	return &Version{
		Major: v.Major + 1,
		Minor: 0,
		Patch: 0,
	}
}

// IncrementMinor creates a new version with incremented minor version
func (v *Version) IncrementMinor() *Version {
	return &Version{
		Major: v.Major,
		Minor: v.Minor + 1,
		Patch: 0,
	}
}

// IncrementPatch creates a new version with incremented patch version
func (v *Version) IncrementPatch() *Version {
	return &Version{
		Major: v.Major,
		Minor: v.Minor,
		Patch: v.Patch + 1,
	}
}

// IsBreakingChange checks if upgrading from old to new version is a breaking change
func IsBreakingChange(oldVersion, newVersion *Version) bool {
	return oldVersion.Major != newVersion.Major
}

// CompareVersions compares two version strings
func CompareVersions(v1Str, v2Str string) (int, error) {
	v1, err := ParseVersion(v1Str)
	if err != nil {
		return 0, err
	}

	v2, err := ParseVersion(v2Str)
	if err != nil {
		return 0, err
	}

	return v1.Compare(v2), nil
}

