package main

import "testing"

// TestIsLoopbackAddr locks the security-critical classification: an empty or
// unspecified host binds all interfaces and must NOT be treated as loopback
// (else the standalone binary would auto-enable unauthenticated access on the
// network). Only explicit loopback hosts are loopback.
func TestIsLoopbackAddr(t *testing.T) {
	for _, c := range []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8080", true},
		{"localhost:8080", true},
		{"[::1]:8080", true},
		{"127.0.0.1", true},
		{":8080", false},          // empty host -> binds 0.0.0.0 / [::]
		{"0.0.0.0:8080", false},   // unspecified IPv4 -> all interfaces
		{"[::]:8080", false},      // unspecified IPv6 -> all interfaces
		{"192.168.1.10:8080", false},
		{"example.com:8080", false},
	} {
		if got := isLoopbackAddr(c.addr); got != c.want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", c.addr, got, c.want)
		}
	}
}
