package core

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const NilUUID UUID = ""

// UUID is a human-readable canonical UUID string that stores as 16 bytes in SQL.
type UUID string

func NewID() UUID {
	if id, err := uuid.NewV7(); err == nil {
		return UUID(id.String())
	}
	return UUID(uuid.New().String())
}

func ParseUUID(s string) (UUID, error) {
	if s == "" {
		return NilUUID, nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return NilUUID, err
	}
	return UUID(id.String()), nil
}

func MustUUID(s string) UUID {
	id, err := ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return id
}

func (u UUID) Value() (driver.Value, error) {
	if u == "" {
		return make([]byte, 16), nil
	}
	id, err := uuid.Parse(string(u))
	if err != nil {
		return nil, err
	}
	return id.MarshalBinary()
}

func (u *UUID) Scan(src any) error {
	if u == nil {
		return fmt.Errorf("core.UUID: Scan on nil pointer")
	}
	switch v := src.(type) {
	case nil:
		*u = NilUUID
		return nil
	case []byte:
		if len(v) != 16 {
			return fmt.Errorf("core.UUID: expected 16 bytes, got %d", len(v))
		}
		if bytes.Equal(v, make([]byte, 16)) {
			*u = NilUUID
			return nil
		}
		id, err := uuid.FromBytes(v)
		if err != nil {
			return err
		}
		*u = UUID(id.String())
		return nil
	case string:
		// Postgres returns a uuid column as its canonical text form, so the
		// 16-zero-byte sentinel arrives here as the nil-uuid string rather than
		// as []byte. Normalize it back to the empty sentinel so the round-trip
		// is identical across dialects ("" -> store -> "").
		if v == "" {
			*u = NilUUID
			return nil
		}
		if id, err := uuid.Parse(v); err == nil {
			if id == uuid.Nil {
				*u = NilUUID
			} else {
				*u = UUID(id.String())
			}
			return nil
		}
		return fmt.Errorf("core.UUID: invalid uuid string %q", v)
	default:
		return fmt.Errorf("core.UUID: cannot scan %T", src)
	}
}

func (UUID) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	switch db.Name() {
	case "postgres":
		return "uuid"
	case "mysql":
		return "binary(16)"
	case "sqlite":
		return "blob"
	default:
		return "blob"
	}
}

func (u UUID) String() string {
	return string(u)
}

func (u UUID) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

func (u *UUID) UnmarshalText(text []byte) error {
	parsed, err := ParseUUID(string(text))
	if err != nil {
		return err
	}
	*u = parsed
	return nil
}

func (u UUID) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

func (u *UUID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return u.UnmarshalText([]byte(s))
}
