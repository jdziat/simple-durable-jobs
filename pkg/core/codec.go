package core

// PayloadCodec transforms serialized payload bytes at the storage boundary.
//
// Implementations receive the JSON bytes already produced by the queue, call,
// or signal layers. They must return plaintext from Decode so the rest of the
// stack continues to operate on ordinary serialized values.
type PayloadCodec interface {
	Encode(plaintext []byte) (stored []byte, err error)
	Decode(stored []byte) (plaintext []byte, err error)
}

type identityCodec struct{}

// IdentityCodec leaves payload bytes unchanged.
//
// It is the default storage codec. Nil inputs return nil outputs.
var IdentityCodec PayloadCodec = identityCodec{}

// NopCodec is an alias for IdentityCodec.
//
// Deprecated: use IdentityCodec.
var NopCodec = IdentityCodec

func (identityCodec) Encode(plaintext []byte) ([]byte, error) {
	return plaintext, nil
}

func (identityCodec) Decode(stored []byte) ([]byte, error) {
	return stored, nil
}
