package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Cross-check with Workstream B: SHA-256 over ASCII "hello" (5 bytes,
// no trailing whitespace) is this exact 64-char hex string. Both
// sides MUST agree on this value or correct submissions will be
// flagged WRONG_ANSWER.
const helloSha256Hex = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

func TestCanonicalize_StripsTrailingWhitespace(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"plain", "hello"},
		{"trailing newline", "hello\n"},
		{"trailing crlf", "hello\r\n"},
		{"trailing spaces", "hello   "},
		{"mixed trailing", "hello \t\r\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := canonicalize(c.in)
			if got != helloSha256Hex {
				t.Fatalf("canonicalize(%q) = %s, want %s", c.in, got, helloSha256Hex)
			}
		})
	}
}

func TestCanonicalize_InteriorWhitespacePreserved(t *testing.T) {
	// Make sure we are NOT trimming leading or interior whitespace —
	// only trailing. "hello world\n" and "hello world" must hash the
	// same; "hello world" and "helloworld" must NOT.
	a := canonicalize("hello world\n")
	b := canonicalize("hello world")
	if a != b {
		t.Fatalf("trailing-newline change altered hash: %s vs %s", a, b)
	}
	c := canonicalize("helloworld")
	if a == c {
		t.Fatalf("hash should differ when interior whitespace differs")
	}
}

func TestRequest_RoundTripJSON(t *testing.T) {
	original := request{
		SessionToken: "11111111-2222-3333-4444-555555555555",
		Language:     "python",
		Code:         "print(int(input()) + 1)\n",
		TimeoutMs:    1000,
		PerTest: []testCaseSpec{
			{Ordinal: 1, Input: "1\n", ExpectedHash: helloSha256Hex},
			{Ordinal: 2, Input: "2\n", ExpectedHash: "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"},
		},
	}
	raw, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded request
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("roundtrip mismatch:\n original = %#v\n decoded  = %#v", original, decoded)
	}
}

func TestResponse_RoundTripJSON(t *testing.T) {
	original := response{
		PerTest: []perTestResult{
			{Ordinal: 1, Verdict: verdictOK, TimeMs: 18, MemoryKb: 2200, StdoutHash: helloSha256Hex},
			{Ordinal: 2, Verdict: verdictWrongAnswer, TimeMs: 22, MemoryKb: 2300, StdoutHash: helloSha256Hex},
		},
		OverallVerdict: verdictWrongAnswer,
	}
	raw, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded response
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("roundtrip mismatch:\n original = %#v\n decoded  = %#v", original, decoded)
	}
}
