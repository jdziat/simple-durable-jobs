// Package codec provides payload codecs, including the secretbox
// encryption-at-rest codec for stored job payloads.
//
// NewSecretbox keeps backward-compatible Decode behavior for legacy plaintext
// rows. NewSecretboxStrict rejects un-prefixed plaintext after migration.
package codec
