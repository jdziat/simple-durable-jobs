package core

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

// nilUUIDString is the canonical text form of the nil UUID. Postgres returns a
// uuid column as this string (not []byte), so Scan must normalize it back to the
// empty NilUUID sentinel for cross-dialect round-trip parity.
const nilUUIDString = "00000000-0000-0000-0000-000000000000"

func TestUUID_Value(t *testing.T) {
	t.Run("canonical string -> 16 bytes", func(t *testing.T) {
		id := UUID("018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b")
		v, err := id.Value()
		if err != nil {
			t.Fatalf("Value() error: %v", err)
		}
		b, ok := v.([]byte)
		if !ok {
			t.Fatalf("Value() = %T, want []byte", v)
		}
		if len(b) != 16 {
			t.Fatalf("len = %d, want 16", len(b))
		}
		want, _ := uuid.Parse(string(id))
		wantBytes, _ := want.MarshalBinary()
		if !bytes.Equal(b, wantBytes) {
			t.Fatalf("bytes = %x, want %x", b, wantBytes)
		}
	})

	t.Run("empty -> 16 zero bytes", func(t *testing.T) {
		v, err := NilUUID.Value()
		if err != nil {
			t.Fatalf("Value() error: %v", err)
		}
		b := v.([]byte)
		if !bytes.Equal(b, make([]byte, 16)) {
			t.Fatalf("empty UUID must encode to 16 zero bytes, got %x", b)
		}
	})

	t.Run("invalid string -> error", func(t *testing.T) {
		if _, err := UUID("not-a-uuid").Value(); err == nil {
			t.Fatal("Value() on a non-UUID string must error")
		}
	})
}

func TestUUID_Scan(t *testing.T) {
	realBytes, _ := uuid.MustParse("018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b").MarshalBinary()

	cases := []struct {
		name    string
		src     any
		want    UUID
		wantErr bool
	}{
		{"nil -> NilUUID", nil, NilUUID, false},
		{"16 bytes -> canonical", realBytes, UUID("018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b"), false},
		{"16 zero bytes -> NilUUID", make([]byte, 16), NilUUID, false},
		{"wrong-length bytes -> error", []byte{0x01, 0x02}, NilUUID, true},
		{"empty string -> NilUUID", "", NilUUID, false},
		// The Postgres path: a uuid column comes back as canonical text.
		{"nil-uuid string -> NilUUID (postgres path)", nilUUIDString, NilUUID, false},
		{"canonical string passthrough", "018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b", UUID("018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b"), false},
		{"invalid string -> error", "garbage", NilUUID, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got UUID
			err := got.Scan(tc.src)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Scan(%v) expected error", tc.src)
				}
				return
			}
			if err != nil {
				t.Fatalf("Scan(%v) error: %v", tc.src, err)
			}
			if got != tc.want {
				t.Fatalf("Scan(%v) = %q, want %q", tc.src, got, tc.want)
			}
		})
	}

	t.Run("Scan on nil pointer errors", func(t *testing.T) {
		var p *UUID
		if err := p.Scan([]byte{}); err == nil {
			t.Fatal("Scan on a nil *UUID must error")
		}
	})
}

// TestUUID_RoundTrip exercises the full Value -> Scan cycle for both the []byte
// path (mysql/sqlite) and the canonical-string path (postgres), for a real id
// and for the empty sentinel. This is the exact behaviour the live-DB suites and
// the chaos INV-SLOT-NO-LEAK invariant depend on.
func TestUUID_RoundTrip(t *testing.T) {
	for _, original := range []UUID{NewID(), NilUUID} {
		v, err := original.Value()
		if err != nil {
			t.Fatalf("Value(%q) error: %v", original, err)
		}
		raw := v.([]byte)

		// []byte path (mysql binary(16), sqlite blob).
		var viaBytes UUID
		if err := viaBytes.Scan(raw); err != nil {
			t.Fatalf("Scan([]byte) error: %v", err)
		}
		if viaBytes != original {
			t.Fatalf("[]byte round-trip = %q, want %q", viaBytes, original)
		}

		// canonical-string path (postgres uuid-as-text). For NilUUID the driver
		// hands back the nil-uuid string; for a real id, the canonical form.
		text := original.String()
		if original == NilUUID {
			text = nilUUIDString
		}
		var viaString UUID
		if err := viaString.Scan(text); err != nil {
			t.Fatalf("Scan(string) error: %v", err)
		}
		if viaString != original {
			t.Fatalf("string round-trip = %q, want %q", viaString, original)
		}
	}
}

func TestUUID_JSONAndText(t *testing.T) {
	id := UUID("018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b")

	b, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}
	if string(b) != `"018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b"` {
		t.Fatalf("MarshalJSON = %s, want canonical string", b)
	}

	var back UUID
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("UnmarshalJSON error: %v", err)
	}
	if back != id {
		t.Fatalf("JSON round-trip = %q, want %q", back, id)
	}

	// Inside a struct, the field renders as the canonical string (wire/dashboard contract).
	type holder struct {
		ID UUID `json:"id"`
	}
	hb, _ := json.Marshal(holder{ID: id})
	if string(hb) != `{"id":"018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b"}` {
		t.Fatalf("struct JSON = %s", hb)
	}

	txt, err := id.MarshalText()
	if err != nil || string(txt) != string(id) {
		t.Fatalf("MarshalText = %q, %v", txt, err)
	}
	var fromText UUID
	if err := fromText.UnmarshalText([]byte(id)); err != nil || fromText != id {
		t.Fatalf("UnmarshalText = %q, %v", fromText, err)
	}
}

func TestNewID_IsUUIDv7(t *testing.T) {
	id := NewID()
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		t.Fatalf("NewID() = %q is not a valid UUID: %v", id, err)
	}
	if v := parsed.Version(); v != 7 {
		t.Fatalf("NewID() version = %d, want 7", v)
	}
	// Two successive ids must differ.
	if NewID() == id {
		t.Fatal("NewID() returned a duplicate")
	}
}

func TestParseUUID(t *testing.T) {
	if got, err := ParseUUID(""); err != nil || got != NilUUID {
		t.Fatalf("ParseUUID(\"\") = %q, %v; want NilUUID", got, err)
	}
	if _, err := ParseUUID("garbage"); err == nil {
		t.Fatal("ParseUUID(garbage) must error")
	}
	// Canonicalizes (uppercase -> lowercase).
	got, err := ParseUUID("018F7C1A-9B2E-7D3F-8A4B-1C2D3E4F5A6B")
	if err != nil || got != UUID("018f7c1a-9b2e-7d3f-8a4b-1c2d3e4f5a6b") {
		t.Fatalf("ParseUUID canonicalization = %q, %v", got, err)
	}
}
