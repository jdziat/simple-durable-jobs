package codec

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretboxDecodeLegacyPlaintext(t *testing.T) {
	var key [32]byte
	key[0] = 1
	c, err := NewSecretbox(key)
	require.NoError(t, err)

	plaintext := []byte(`{"legacy":true}`)
	decoded, err := c.Decode(plaintext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded)
}

func TestSecretboxStrictRejectsLegacyPlaintext(t *testing.T) {
	var key [32]byte
	key[0] = 1
	c, err := NewSecretboxStrict(key)
	require.NoError(t, err)

	_, err = c.Decode([]byte(`{"legacy":true}`))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSecretboxLegacyPlaintext))
}

func TestNewSecretboxRejectsZeroPrimaryKey(t *testing.T) {
	var key [32]byte

	c, err := NewSecretbox(key)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "primary key must not be all zero")

	c, err = NewSecretboxStrict(key)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "primary key must not be all zero")
}

func TestSecretboxRoundTrip(t *testing.T) {
	var key [32]byte
	key[0] = 1
	c, err := NewSecretbox(key)
	require.NoError(t, err)

	plaintext := []byte(`{"secret":"value"}`)
	stored, err := c.Encode(plaintext)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, stored)
	require.True(t, bytes.HasPrefix(stored, magic))

	decoded, err := c.Decode(stored)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded)
}

func TestSecretboxStrictRoundTrip(t *testing.T) {
	var key [32]byte
	key[0] = 1
	c, err := NewSecretboxStrict(key)
	require.NoError(t, err)

	plaintext := []byte(`{"secret":"value"}`)
	stored, err := c.Encode(plaintext)
	require.NoError(t, err)

	decoded, err := c.Decode(stored)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded)
}

func TestSecretboxKeyRotationFallback(t *testing.T) {
	var oldKey [32]byte
	oldKey[0] = 1
	oldCodec, err := NewSecretbox(oldKey)
	require.NoError(t, err)

	plaintext := []byte(`{"rotated":true}`)
	stored, err := oldCodec.Encode(plaintext)
	require.NoError(t, err)

	var newKey [32]byte
	newKey[0] = 2
	rotatedCodec, err := NewSecretbox(newKey, oldKey)
	require.NoError(t, err)

	decoded, err := rotatedCodec.Decode(stored)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded)
}

func TestSecretboxAuthenticationFailure(t *testing.T) {
	var key [32]byte
	key[0] = 1
	c, err := NewSecretbox(key)
	require.NoError(t, err)

	stored, err := c.Encode([]byte("secret"))
	require.NoError(t, err)
	stored[len(stored)-1] ^= 0xff

	_, err = c.Decode(stored)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSecretboxAuthentication))
}
