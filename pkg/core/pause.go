package core

// PauseMode determines how pause operations behave.
type PauseMode string

const (
	// PauseModeGraceful lets running jobs complete, stops new jobs from being picked up.
	PauseModeGraceful PauseMode = "graceful"
	// PauseModeAggressive interrupts running jobs immediately via context cancellation.
	PauseModeAggressive PauseMode = "aggressive"
)
