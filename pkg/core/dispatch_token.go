package core

import "context"

// WithDispatchToken binds the durable incarnation returned by Dequeue to an
// execution context. GormStorage uses it in addition to worker ID for every
// ownership-guarded lifecycle write. It is deliberately additive: custom v4
// Storage implementations continue to receive the existing method calls.
func WithDispatchToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, dispatchTokenKey{}, token)
}

// DispatchTokenFromContext returns the current dequeue incarnation.
func DispatchTokenFromContext(ctx context.Context) (string, bool) {
	token, ok := ctx.Value(dispatchTokenKey{}).(string)
	return token, ok && token != ""
}

type dispatchTokenKey struct{}
