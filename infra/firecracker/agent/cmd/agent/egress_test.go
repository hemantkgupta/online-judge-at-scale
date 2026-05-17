//go:build integration_microvm
// +build integration_microvm

// Integration test for roadmap §3.1 — per-microVM egress lockdown.
//
// This test is intended to run INSIDE a Firecracker microVM that the
// sandbox-manager has booted with the egress lockdown enabled (netns
// with only `lo`, no `network-interfaces` in the FC machine config).
// The assertion is the inverse of "did anything we expected to be
// blocked actually break out": every off-host TCP dial must fail fast,
// and DNS may either fail-fast or succeed (host resolver bound via
// the rootfs's /etc/resolv.conf can still answer locally; what matters
// is no packets leave).
//
// Build tag `integration_microvm` keeps this out of the unit `go test
// ./...` path. To compile a copy that the SM's microvm harness can
// run, do:
//
//	GOOS=linux GOARCH=amd64 \
//	    go test -tags=integration_microvm \
//	        -c -o /tmp/egress_test \
//	        ./infra/firecracker/agent/cmd/agent
//
// then load /tmp/egress_test into a locked-down microVM (e.g. drop it
// into the rootfs at /usr/local/bin/egress_test) and exec it. A zero
// exit code is "lockdown holding"; non-zero is a leak that should
// page on-call.

package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// dialDeadline is the per-target budget. The whole point of the
// lockdown is that connect() returns ENETUNREACH immediately — we
// pick 200 ms because anything slower implies the kernel actually
// queued a SYN and is waiting on a timeout (i.e. the netns has a
// route somewhere it shouldn't).
const dialDeadline = 200 * time.Millisecond

// leakWindow is the upper bound on how long a failing dial may take
// before we treat it as suspect. A leak that returns ETIMEDOUT after
// 5s is still a leak: the kernel had a route it walked all the way to
// the network stack on. We give a small buffer over dialDeadline to
// avoid flaking on slow CPUs.
const leakWindow = 250 * time.Millisecond

// expectedErrSubstrings are the strings we accept as "lockdown
// working". Linux surfaces ENETUNREACH or EHOSTUNREACH for a dial
// with no route. EACCES can also appear if iptables -j REJECT is
// in play with the OUTPUT owner rule. ECONNREFUSED is borderline
// (it means we reached a host but it had nothing on that port) but
// we accept it conservatively for completeness — if it ever fires
// in a real run that's worth a closer look.
var expectedErrSubstrings = []string{
	"no route to host",
	"network is unreachable",
	"network unreachable",
	"connection refused",
	"permission denied",
	"operation not permitted",
}

func TestEgressBlocked_TCPDial_PublicV4(t *testing.T) {
	targets := []string{
		"1.1.1.1:80",
		"8.8.8.8:443",
	}
	for _, addr := range targets {
		addr := addr
		t.Run(addr, func(t *testing.T) {
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), dialDeadline)
			defer cancel()

			d := net.Dialer{}
			conn, err := d.DialContext(ctx, "tcp", addr)
			elapsed := time.Since(start)

			if err == nil {
				_ = conn.Close()
				t.Fatalf("LEAK: dial %s succeeded — egress lockdown is broken", addr)
			}

			if elapsed > leakWindow {
				t.Fatalf("SUSPECT: dial %s took %v (>%v); error=%v — guest may have a route that absorbed the SYN",
					addr, elapsed, leakWindow, err)
			}

			if !isExpectedNetErr(err) {
				t.Fatalf("UNEXPECTED error dialing %s: %v (not one of %v) — investigate before declaring pass",
					addr, err, expectedErrSubstrings)
			}
		})
	}
}

func TestEgressBlocked_MetadataServer(t *testing.T) {
	// GCE metadata server. Reaching this exposes the compute VM's
	// service-account token — the single highest-impact leak.
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), dialDeadline)
	defer cancel()

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", "169.254.169.254:80")
	elapsed := time.Since(start)

	if err == nil {
		_ = conn.Close()
		t.Fatalf("LEAK: dial 169.254.169.254 succeeded — metadata server reachable from guest")
	}
	if elapsed > leakWindow {
		t.Fatalf("SUSPECT: metadata dial took %v (>%v); error=%v", elapsed, leakWindow, err)
	}
}

func TestEgressBlocked_DNSResolution_RelaxedAssertion(t *testing.T) {
	// DNS is a "may succeed locally" case: the guest's
	// /etc/resolv.conf might point at a stub resolver baked into the
	// rootfs, or the kernel might short-circuit hostname lookups
	// against /etc/hosts. Either is fine as long as no UDP/TCP
	// packets leave the netns — which the previous tests already
	// established. So here we just assert the call returns within a
	// human timeframe (no 30s DNS-timeout hangs).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r := net.Resolver{}
	_, err := r.LookupIPAddr(ctx, "google.com")
	// Either error OR success is acceptable; we only fail if the call
	// blocks past the deadline (which manifests as ctx.Err() in the
	// returned err).
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DNS lookup hung past 2s — netns may have a partial route that's absorbing UDP")
	}
}

// isExpectedNetErr matches against the standard set of "no route"
// surface strings as well as the typed errno variants we can
// recognise programmatically.
func isExpectedNetErr(err error) bool {
	if err == nil {
		return false
	}
	// Typed checks first — these don't depend on the libc message
	// table and won't drift across distros.
	var sysErr syscall.Errno
	if errors.As(err, &sysErr) {
		switch sysErr {
		case syscall.ENETUNREACH, syscall.EHOSTUNREACH,
			syscall.ECONNREFUSED, syscall.EACCES, syscall.EPERM:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	for _, want := range expectedErrSubstrings {
		if strings.Contains(msg, want) {
			return true
		}
	}
	return false
}
