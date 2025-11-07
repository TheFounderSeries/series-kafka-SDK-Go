// Package contracts provides schema migration support
package contracts

import (
	"fmt"

	sdkerrors "github.com/TheFounderSeries/series-kafka-go/pkg/errors"
)

// Migration represents a schema migration from one version to another
type Migration struct {
	FromVersion *Version
	ToVersion   *Version
	Description string
	Transform   func(interface{}) (interface{}, error)
}

// NewMigration creates a new migration
func NewMigration(fromVersion, toVersion string, transform func(interface{}) (interface{}, error)) (*Migration, error) {
	from, err := ParseVersion(fromVersion)
	if err != nil {
		return nil, err
	}

	to, err := ParseVersion(toVersion)
	if err != nil {
		return nil, err
	}

	if !from.IsLessThan(to) {
		return nil, sdkerrors.NewValidationError("version",
			"fromVersion must be less than toVersion",
			nil)
	}

	return &Migration{
		FromVersion: from,
		ToVersion:   to,
		Transform:   transform,
	}, nil
}

// Apply applies the migration transformation to data
func (m *Migration) Apply(data interface{}) (interface{}, error) {
	if m.Transform == nil {
		return data, nil // No transformation needed
	}

	return m.Transform(data)
}

// IsApplicable checks if this migration is applicable for the given version range
func (m *Migration) IsApplicable(currentVersion string) (bool, error) {
	current, err := ParseVersion(currentVersion)
	if err != nil {
		return false, err
	}

	return current.Equals(m.FromVersion), nil
}

// MigrationPath represents a sequence of migrations
type MigrationPath struct {
	migrations []*Migration
}

// NewMigrationPath creates a new migration path
func NewMigrationPath() *MigrationPath {
	return &MigrationPath{
		migrations: []*Migration{},
	}
}

// AddMigration adds a migration to the path
func (mp *MigrationPath) AddMigration(migration *Migration) error {
	// Validate that migrations form a connected path
	if len(mp.migrations) > 0 {
		lastMigration := mp.migrations[len(mp.migrations)-1]
		if !lastMigration.ToVersion.Equals(migration.FromVersion) {
			return sdkerrors.NewValidationError("migration",
				"migrations must form a connected path",
				nil)
		}
	}

	mp.migrations = append(mp.migrations, migration)
	return nil
}

// Apply applies all migrations in the path
func (mp *MigrationPath) Apply(data interface{}) (interface{}, error) {
	result := data
	var err error

	for _, migration := range mp.migrations {
		result, err = migration.Apply(result)
		if err != nil {
			return nil, fmt.Errorf("migration from %s to %s failed: %w",
				migration.FromVersion.String(),
				migration.ToVersion.String(),
				err)
		}
	}

	return result, nil
}

// FromVersion returns the starting version of the path
func (mp *MigrationPath) FromVersion() *Version {
	if len(mp.migrations) == 0 {
		return nil
	}
	return mp.migrations[0].FromVersion
}

// ToVersion returns the ending version of the path
func (mp *MigrationPath) ToVersion() *Version {
	if len(mp.migrations) == 0 {
		return nil
	}
	return mp.migrations[len(mp.migrations)-1].ToVersion
}

// IsEmpty checks if the migration path is empty
func (mp *MigrationPath) IsEmpty() bool {
	return len(mp.migrations) == 0
}

// Length returns the number of migrations in the path
func (mp *MigrationPath) Length() int {
	return len(mp.migrations)
}

// FieldMapping represents a mapping of fields during migration
type FieldMapping struct {
	oldField string
	newField string
	transform func(interface{}) (interface{}, error)
}

// NewFieldMapping creates a new field mapping
func NewFieldMapping(oldField, newField string, transform func(interface{}) (interface{}, error)) *FieldMapping {
	return &FieldMapping{
		oldField:  oldField,
		newField:  newField,
		transform: transform,
	}
}

// Apply applies the field mapping to data
func (fm *FieldMapping) Apply(data map[string]interface{}) error {
	value, exists := data[fm.oldField]
	if !exists {
		return nil // Field doesn't exist, skip
	}

	// Apply transformation if provided
	if fm.transform != nil {
		transformed, err := fm.transform(value)
		if err != nil {
			return err
		}
		value = transformed
	}

	// Set new field
	data[fm.newField] = value

	// Remove old field if different from new field
	if fm.oldField != fm.newField {
		delete(data, fm.oldField)
	}

	return nil
}

