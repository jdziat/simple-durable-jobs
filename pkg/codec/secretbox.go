package codec

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/nacl/secretbox"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

var (
	magic = []byte{'s', 'd', 'j', '1'}

	// ErrSecretboxAuthentication is returned when a versioned ciphertext cannot
	// be authenticated with the primary key or any fallback key.
	ErrSecretboxAuthentication = errors.New("jobs: secretbox payload authentication failed")
)

// Secretbox encrypts payload bytes with NaCl secretbox.
//
// Values without the sdj1 magic prefix are treated as legacy plaintext and
// returned unchanged by Decode, enabling mixed plaintext/encrypted databases.
type Secretbox struct {
	primary   [32]byte
	fallbacks [][32]byte
}

var _ core.PayloadCodec = (*Secretbox)(nil)

// NewSecretbox creates a codec using primaryKey for new writes and fallbackKeys
// only for decrypting older ciphertext after key rotation.
func NewSecretbox(primaryKey [32]byte, fallbackKeys ...[32]byte) (*Secretbox, error) {
	cp := make([][32]byte, len(fallbackKeys))
	copy(cp, fallbackKeys)
	return &Secretbox{primary: primaryKey, fallbacks: cp}, nil
}

// Encode encrypts plaintext with a fresh random nonce.
func (s *Secretbox) Encode(plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 {
		return plaintext, nil
	}
	var nonce [24]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, fmt.Errorf("jobs: secretbox nonce: %w", err)
	}
	out := make([]byte, 0, len(magic)+len(nonce)+secretbox.Overhead+len(plaintext))
	out = append(out, magic...)
	out = append(out, nonce[:]...)
	out = secretbox.Seal(out, plaintext, &nonce, &s.primary)
	return out, nil
}

// Decode decrypts versioned ciphertext, or returns un-prefixed legacy plaintext
// unchanged.
func (s *Secretbox) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return stored, nil
	}
	if !bytes.HasPrefix(stored, magic) {
		return stored, nil
	}
	if len(stored) < len(magic)+24+secretbox.Overhead {
		return nil, fmt.Errorf("%w: truncated payload", ErrSecretboxAuthentication)
	}
	var nonce [24]byte
	copy(nonce[:], stored[len(magic):len(magic)+24])
	ciphertext := stored[len(magic)+24:]

	keys := make([][32]byte, 0, 1+len(s.fallbacks))
	keys = append(keys, s.primary)
	keys = append(keys, s.fallbacks...)
	for i := range keys {
		plaintext, ok := secretbox.Open(nil, ciphertext, &nonce, &keys[i])
		if ok {
			return plaintext, nil
		}
	}
	return nil, ErrSecretboxAuthentication
}
