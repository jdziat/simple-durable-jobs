package core

// DeadLetterFilter scopes dead-letter triage queries.
type DeadLetterFilter struct {
	Queue string
	Type  string
	// Tenant matches dead-lettered jobs owned by exactly this tenant.
	Tenant string
	// MetaContains requires every key/value pair to appear in the job metadata.
	MetaContains *MetadataMap
	Search       string
	Limit        int
	Offset       int
}
