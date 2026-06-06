---
title: "Payload Codec"
weight: 8
toc: true
---

Simple Durable Jobs can transform payload bytes before they are written to the
database. This is useful for encrypting job arguments, job results, checkpoint
results, and signal payloads at rest without changing handler code.

## Interface

Payload codecs operate on serialized bytes:

```go
type PayloadCodec interface {
	Encode(plaintext []byte) (stored []byte, err error)
	Decode(stored []byte) (plaintext []byte, err error)
}
```

JSON marshaling and unmarshaling still happen in the queue, call, and signal
layers. The codec only transforms the resulting `[]byte` at the GORM storage
boundary.

## Configure GORM storage

Pass a codec when creating storage:

```go
store := jobs.NewGormStorage(db, jobs.WithCodec(myCodec))
```

If no codec is configured, storage uses `jobs.IdentityCodec`, which returns
bytes unchanged. Nil or empty payloads are not encoded or decoded.

## Secretbox encryption

The built-in codec uses NaCl Secretbox:

```go
var key [32]byte
copy(key[:], []byte("32-byte-secret-key-material-here!!"))

codec, err := jobs.NewSecretbox(key)
if err != nil {
	return err
}

store := jobs.NewGormStorage(db, jobs.WithCodec(codec))
```

Each write uses a fresh random nonce. Stored ciphertext is prefixed with a
stable version marker so the codec can distinguish encrypted payloads from
legacy plaintext.

## Zero-migration rollout

Secretbox decode is intentionally tolerant: if stored bytes do not have the
Secretbox marker, they are returned unchanged as legacy plaintext. This means
you can enable the codec on an existing database without rewriting old rows.
New writes are encrypted; old rows continue to read normally.

No database migration is required. Existing `[]byte` payload columns already
store the encoded bytes.

## Key rotation

New writes use the primary key. Older keys can be supplied as decrypt-only
fallbacks:

```go
codec, err := jobs.NewSecretbox(newKey, oldKey)
```

Rows encrypted with `oldKey` continue to decrypt. Rows written after rotation
use `newKey`.

## Operational notes

Codec-encoded payloads are opaque to database-side filtering and inspection.
The dashboard and application APIs still receive decoded plaintext because they
read through the storage layer, but direct SQL queries see ciphertext for
encoded rows.
