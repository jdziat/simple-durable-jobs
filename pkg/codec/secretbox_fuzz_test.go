package codec

import (
	"bytes"
	"testing"
)

func fuzzSecretbox(t testing.TB) *Secretbox {
	t.Helper()
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	s, err := NewSecretbox(key)
	if err != nil {
		t.Fatalf("NewSecretbox: %v", err)
	}
	return s
}

func FuzzSecretboxRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("hello"))
	f.Add([]byte{'n', 'u', 'l', 0, 'b', 'y', 't', 'e'})
	f.Add(append(append([]byte{}, magic...), []byte("plaintext-with-magic-prefix")...))

	f.Fuzz(func(t *testing.T, plaintext []byte) {
		s := fuzzSecretbox(t)
		enc, err := s.Encode(plaintext)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		dec, err := s.Decode(enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if !bytes.Equal(dec, plaintext) {
			t.Fatalf("Decode(Encode(x)) mismatch: got %q want %q", dec, plaintext)
		}
	})
}

func FuzzSecretboxDecodeNoPanic(f *testing.F) {
	s := fuzzSecretbox(f)
	valid, err := s.Encode([]byte("valid ciphertext seed"))
	if err != nil {
		f.Fatalf("Encode seed: %v", err)
	}
	flipped := append([]byte{}, valid...)
	flipped[len(flipped)-1] ^= 0xff

	f.Add([]byte{0xff, 0x00, 0x42, 0x99, 0x10})
	f.Add(append(append([]byte{}, magic...), []byte("short")...))
	f.Add(flipped)

	f.Fuzz(func(t *testing.T, data []byte) {
		s := fuzzSecretbox(t)
		_, _ = s.Decode(data)
	})
}
