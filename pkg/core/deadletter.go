package core

// DeadLetterFilter scopes dead-letter triage queries.
type DeadLetterFilter struct {
	Queue  string
	Type   string
	Limit  int
	Offset int
}
