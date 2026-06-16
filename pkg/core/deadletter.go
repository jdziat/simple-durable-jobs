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
	// SortKey/SortDir select a whitelisted order column (see GormStorage). When
	// SortKey is empty the default dead-letter order (dead_lettered_at DESC) is
	// kept so the most-recently-dead jobs surface first.
	SortKey string
	SortDir string
}
