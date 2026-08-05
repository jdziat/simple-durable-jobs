package typed_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/typed"
	"github.com/stretchr/testify/require"
)

type greetArgs struct {
	Name string `json:"name"`
}
type greetResult struct {
	Greeting string `json:"greeting"`
}
type mailArgs struct {
	Email string `json:"email"`
}
type mailResult struct {
	Sent bool `json:"sent"`
}

// Two Defines sharing a name used to be last-write-wins: the second silently
// replaced the first. After that, a typed Call through the FIRST definition
// JSON-round-tripped the caller's argument into the second's argument struct. With
// no shared field names that decodes cleanly to the zero value, so the callee saw
// a zero arg, its result decoded back to a zero result, and the job COMPLETED with
// a nil error on every observable surface — silent wrong data, undetectable at
// runtime.
//
// Only Def.Enqueue was guarded (ValidateArgs -> ErrJobArgsMismatch). Def.Call,
// EnqueueRemote, and any job already queued under the first definition were not.
//
// The library already refuses the analogous duplicate for SCHEDULES
// ("schedule already registered for %q"), so handler names were the outlier.
func TestDefine_SameNameDifferentTypesIsRefused(t *testing.T) {
	q := queue.New(nil)

	_, err := typed.DefineE[greetArgs, greetResult](q, "collide",
		func(context.Context, greetArgs) (greetResult, error) { return greetResult{}, nil })
	require.NoError(t, err, "premise: the first definition must register")

	_, err = typed.DefineE[mailArgs, mailResult](q, "collide",
		func(context.Context, mailArgs) (mailResult, error) { return mailResult{}, nil })
	require.Error(t, err,
		"a second Define under the same name with DIFFERENT types must be refused; silently replacing the first makes typed calls through it return zero values with a nil error")
	require.Contains(t, err.Error(), "already registered with a different signature",
		"the error must say what is wrong so the author can fix the name: %v", err)
}

// Re-registering the SAME name with the SAME signature stays permitted. The guard
// is scoped to a signature change on purpose — refusing every duplicate would break
// legitimate re-registration, such as a queue rebuilt from the same definitions in
// test setup.
func TestDefine_SameNameSameTypesRemainsIdempotent(t *testing.T) {
	q := queue.New(nil)

	_, err := typed.DefineE[greetArgs, greetResult](q, "stable",
		func(context.Context, greetArgs) (greetResult, error) { return greetResult{}, nil })
	require.NoError(t, err)

	_, err = typed.DefineE[greetArgs, greetResult](q, "stable",
		func(context.Context, greetArgs) (greetResult, error) {
			return greetResult{Greeting: "second"}, nil
		})
	require.NoError(t, err,
		"re-registering the same name with the same argument and result types must remain allowed; the guard targets a signature CHANGE, not duplication")
}

// A different name is never affected.
func TestDefine_DistinctNamesAreUnaffected(t *testing.T) {
	q := queue.New(nil)
	_, err := typed.DefineE[greetArgs, greetResult](q, "one",
		func(context.Context, greetArgs) (greetResult, error) { return greetResult{}, nil })
	require.NoError(t, err)
	_, err = typed.DefineE[mailArgs, mailResult](q, "two",
		func(context.Context, mailArgs) (mailResult, error) { return mailResult{}, nil })
	require.NoError(t, err)
}
