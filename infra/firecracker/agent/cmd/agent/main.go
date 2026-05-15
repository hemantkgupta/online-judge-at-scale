// Package main implements oj-execution-agent, the tiny trusted process
// that runs inside every Firecracker microVM. See Part 7 of the blog —
// the agent's contract is deliberately boring:
//
//	receive one structured request, run all the test cases, return one
//	structured response.
//
// The agent listens on AF_VSOCK port AGENT_PORT (default 1234). Each
// connection from the host is one job (one submission, N test cases):
//
//	host → agent     {"session_token","language","code","time_limit_ms","per_test":[...]}\n
//	agent → host     {"per_test":[...], "overall_verdict":"...", "detail":""}\n
//
// One job per connection — the host closes the socket after reading the
// response, the agent loops back to Accept(). PID 1 is the /init script
// in the rootfs which just execs this binary, so when the agent exits
// the kernel panics and Firecracker exits, which is how the Sandbox
// Manager learns the VM has finished.
//
// Design pivot (Workstream CD): Firecracker REST does not support live
// drive hot-attach on warm-pool VMs, so we send code + test inputs over
// vsock (Option α). The host worker hashes the expected stdout of each
// test case (SHA-256 over strings.TrimRight(stdout, " \t\r\n"), lowercase
// hex). The agent runs each test case, hashes its own stdout the same
// way, compares to the expected hash, and returns per-test verdicts.
// Nothing is mounted at /code — the per-test inputs live in memory and
// are written to a per-job tmpfs workdir.
//
// Why Go? The agent runs next to hostile code; we want a tight static
// binary with no runtime to attack. The agent never touches Kafka, R2,
// CockroachDB, or Redis — see the blog's trust-zone table. Anything the
// host needs comes back as structured fields, never raw bytes.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mdlayher/vsock"
)

const (
	agentPort = 1234

	// Hard cap on what we send back inside any single per-test result
	// (e.g., compile diagnostics). The blog stresses "structured fields,
	// never raw bytes": the host should not be parsing megabytes of
	// attacker-controlled output. Per-test stdout itself is hashed and
	// not returned in full; only the hash crosses the trust boundary.
	maxOutputBytes = 64 * 1024

	// Floor on per-job wall clock so the host's host-side watchdog
	// (Phase E) is always the longer of the two timers. Inside the
	// agent we set ctx with the contestant's timeout; we still need
	// some headroom for compile + setup.
	minWallSeconds = 2
	maxWallSeconds = 60

	// MMDS endpoint where Firecracker injects the per-VM session token.
	// We do a short-timeout GET on every job; the result is cached for
	// the lifetime of the agent (one VM = one session anyway).
	mmdsTokenURL = "http://169.254.169.254/latest/oj/session-token"

	// Environment variable used as a dev/test override for the expected
	// session token. If neither env nor MMDS supplies a value, the
	// agent accepts any token (and logs a warning).
	envSessionToken = "OJ_SESSION_TOKEN"
)

// Verdict strings. Kept as constants so the worker-side enum (Workstream B)
// and the agent stay in sync.
const (
	verdictOK            = "OK"
	verdictWrongAnswer   = "WRONG_ANSWER"
	verdictRuntimeError  = "RUNTIME_ERROR"
	verdictTimeout       = "TIMEOUT"
	verdictCompileError  = "COMPILE_ERROR"
	verdictInternalError = "INTERNAL_ERROR"
)

type request struct {
	SessionToken string         `json:"session_token"`
	Language     string         `json:"language"`
	Code         string         `json:"code"`
	TimeoutMs    int            `json:"time_limit_ms"`
	PerTest      []testCaseSpec `json:"per_test"`
}

type testCaseSpec struct {
	Ordinal      int    `json:"ordinal"`
	Input        string `json:"input"`
	ExpectedHash string `json:"expected_hash"`
}

type response struct {
	PerTest        []perTestResult `json:"per_test"`
	OverallVerdict string          `json:"overall_verdict"`
	Detail         string          `json:"detail,omitempty"`
}

type perTestResult struct {
	Ordinal    int    `json:"ordinal"`
	Verdict    string `json:"verdict"`
	TimeMs     int64  `json:"time_ms"`
	MemoryKb   int64  `json:"memory_kb"`
	StdoutHash string `json:"stdout_hash"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[agent] starting")

	l, err := vsock.Listen(agentPort, nil)
	if err != nil {
		log.Fatalf("[agent] vsock.Listen(%d): %v", agentPort, err)
	}
	defer l.Close()
	log.Printf("[agent] listening on vsock port %d", agentPort)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("[agent] Accept: %v — exiting", err)
			return
		}
		// One job per connection. Sequential, never concurrent — the VM
		// is single-tenant by construction (one submission per VM).
		handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReaderSize(conn, 1<<20) // 1 MiB request cap — solutions are tiny
	raw, err := br.ReadBytes('\n')
	if err != nil {
		writeResp(conn, internalError("read request: "+err.Error()))
		return
	}

	var req request
	if err := json.Unmarshal(raw, &req); err != nil {
		writeResp(conn, internalError("parse request: "+err.Error()))
		return
	}

	resp := runJob(req)
	writeResp(conn, resp)
}

func writeResp(conn net.Conn, r response) {
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&r); err != nil {
		log.Printf("[agent] write response: %v", err)
	}
}

// internalError is a small convenience for the "we never started running
// test cases" case. Per the wire contract this is overall_verdict =
// INTERNAL_ERROR with an empty per_test array.
func internalError(detail string) response {
	return response{
		PerTest:        []perTestResult{},
		OverallVerdict: verdictInternalError,
		Detail:         detail,
	}
}

// runJob materialises the contestant payload, compiles once, then runs
// every per-test case sequentially. No I/O outside /tmp; no networking
// other than the one MMDS GET; no filesystem outside the per-job workdir.
func runJob(req request) response {
	// --- session-token gate ------------------------------------------------
	expected := getExpectedSessionToken()
	if expected == "" {
		log.Printf("[agent] WARN: no expected session token configured (MMDS or %s); accepting any token", envSessionToken)
	} else if req.SessionToken != expected {
		return response{
			PerTest:        []perTestResult{},
			OverallVerdict: verdictInternalError,
			Detail:         "session_token mismatch",
		}
	}

	if len(req.PerTest) == 0 {
		return internalError("no test cases in request")
	}

	timeoutMs := req.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	wallSeconds := (timeoutMs / 1000) + 2 // generous compile/spawn headroom
	if wallSeconds < minWallSeconds {
		wallSeconds = minWallSeconds
	}
	if wallSeconds > maxWallSeconds {
		wallSeconds = maxWallSeconds
	}

	workdir, err := os.MkdirTemp("/tmp", "oj-job-")
	if err != nil {
		return internalError("mkdir: " + err.Error())
	}
	defer os.RemoveAll(workdir)

	// --- compile once (or just stage source for python) -------------------
	argv, compErr := compile(workdir, req, time.Duration(wallSeconds)*time.Second)
	if compErr != nil {
		// COMPILE_ERROR: single per-test entry, compiler diagnostics in
		// stdout_hash slot? No — the wire contract gives no place for
		// diagnostics on a per-test entry. Surface them in overall detail.
		return response{
			PerTest: []perTestResult{{
				Ordinal: 0,
				Verdict: verdictCompileError,
			}},
			OverallVerdict: verdictCompileError,
			Detail:         truncate(compErr.diagnostics),
		}
	}

	// --- per-test loop ----------------------------------------------------
	results := make([]perTestResult, 0, len(req.PerTest))
	overall := verdictOK
	for _, tc := range req.PerTest {
		r := runOneTest(workdir, argv, tc,
			time.Duration(timeoutMs)*time.Millisecond,
			time.Duration(wallSeconds)*time.Second)
		results = append(results, r)
		if overall == verdictOK && r.Verdict != verdictOK {
			overall = r.Verdict
		}
	}

	return response{PerTest: results, OverallVerdict: overall}
}

// compileResult is just the diagnostics text from a failed compile.
type compileResult struct {
	diagnostics string
}

// compile stages the contestant source and (for java/cpp) runs the
// compiler. Returns the argv to execute per test case, or a
// non-nil *compileResult if compilation failed.
func compile(workdir string, req request, wallTimeout time.Duration) ([]string, *compileResult) {
	switch req.Language {
	case "python":
		src := filepath.Join(workdir, "solution.py")
		if err := os.WriteFile(src, []byte(req.Code), 0o644); err != nil {
			return nil, &compileResult{diagnostics: "write source: " + err.Error()}
		}
		return []string{"python3", src}, nil

	case "java":
		src := filepath.Join(workdir, "Solution.java")
		if err := os.WriteFile(src, []byte(req.Code), 0o644); err != nil {
			return nil, &compileResult{diagnostics: "write source: " + err.Error()}
		}
		ctx, cancel := context.WithTimeout(context.Background(), wallTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "javac", "-d", workdir, src)
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, &compileResult{diagnostics: string(out)}
		}
		return []string{"java", "-cp", workdir, "Solution"}, nil

	case "cpp":
		src := filepath.Join(workdir, "solution.cpp")
		if err := os.WriteFile(src, []byte(req.Code), 0o644); err != nil {
			return nil, &compileResult{diagnostics: "write source: " + err.Error()}
		}
		bin := filepath.Join(workdir, "a.out")
		ctx, cancel := context.WithTimeout(context.Background(), wallTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "g++", "-O2", "-pipe", "-static", "-o", bin, src)
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, &compileResult{diagnostics: string(out)}
		}
		return []string{bin}, nil

	default:
		return nil, &compileResult{diagnostics: "unsupported language: " + req.Language}
	}
}

// runOneTest executes argv once with tc.Input on stdin, enforces the
// per-test time limit, hashes stdout, and produces a per-test result.
func runOneTest(workdir string, argv []string, tc testCaseSpec, runTimeout, wallTimeout time.Duration) perTestResult {
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = workdir
	cmd.Stdin = strings.NewReader(tc.Input)

	// stdout and stderr go into the SAME limited buffer — this matches the
	// pre-pivot behaviour. We hash whatever the contestant printed; if
	// they spam stderr we still cap the buffer at maxOutputBytes.
	cap := &limitedBuffer{limit: maxOutputBytes}
	var mu sync.Mutex
	cmd.Stdout = &lockedWriter{w: cap, mu: &mu}
	cmd.Stderr = &lockedWriter{w: cap, mu: &mu}

	// SysProcAttr: new pgrp so a context-canceled run kills children too.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Snapshot peak-RSS BEFORE so we can attribute just this child's
	// allocation. RUSAGE_CHILDREN is cumulative across all reaped children,
	// so per-test deltas are the best we can do from getrusage.
	rssBefore := peakRssKb()

	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)

	rssAfter := peakRssKb()
	memKb := rssAfter - rssBefore
	if memKb < 0 {
		memKb = rssAfter
	}

	verdict := verdictOK
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			verdict = verdictTimeout
		} else if _, ok := err.(*exec.ExitError); ok {
			verdict = verdictRuntimeError
		} else {
			// Couldn't even spawn the process — treat as runtime error
			// rather than failing the entire submission with INTERNAL_ERROR.
			verdict = verdictRuntimeError
		}
	}

	stdoutHash := canonicalize(cap.String())
	if verdict == verdictOK && stdoutHash != tc.ExpectedHash {
		verdict = verdictWrongAnswer
	}

	return perTestResult{
		Ordinal:    tc.Ordinal,
		Verdict:    verdict,
		TimeMs:     elapsed.Milliseconds(),
		MemoryKb:   memKb,
		StdoutHash: stdoutHash,
	}
}

// canonicalize computes the lowercase-hex SHA-256 of the contestant
// stdout AFTER trimming trailing whitespace (" \t\r\n"). This MUST
// match Workstream B's hash-of-expected-output canonicalization
// exactly, otherwise correct submissions are flagged WRONG_ANSWER.
//
// Worker side uses Java's String.stripTrailing() which strips any
// Unicode whitespace; Go's strings.TrimRight(s, " \t\r\n") is
// stricter, but in practice contestant programs do not emit exotic
// whitespace (NBSP, vertical-tab, etc.) at the tail of their output.
func canonicalize(stdout string) string {
	trimmed := strings.TrimRight(stdout, " \t\r\n")
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:])
}

// getExpectedSessionToken returns the token the agent should require
// from the host on every request. Lookup order:
//  1. OJ_SESSION_TOKEN env var (dev / test override)
//  2. Firecracker MMDS at /latest/oj/session-token
//  3. "" — token validation disabled (dev-friendly, logs a warning)
//
// The result is cached for the lifetime of the agent: one VM lives
// for exactly one submission, so the token never changes.
var (
	sessionTokenOnce sync.Once
	sessionTokenVal  string
)

func getExpectedSessionToken() string {
	sessionTokenOnce.Do(func() {
		if v := strings.TrimSpace(os.Getenv(envSessionToken)); v != "" {
			sessionTokenVal = v
			log.Printf("[agent] session token loaded from $%s", envSessionToken)
			return
		}
		// MMDS: short timeout — we never want a slow MMDS to extend
		// the cold-start path. If MMDS isn't reachable, leave the
		// token empty (validation disabled).
		client := &http.Client{Timeout: 250 * time.Millisecond}
		resp, err := client.Get(mmdsTokenURL)
		if err != nil {
			log.Printf("[agent] MMDS unreachable (%v) — session token validation disabled", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("[agent] MMDS returned %d — session token validation disabled", resp.StatusCode)
			return
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("[agent] MMDS read body: %v — session token validation disabled", err)
			return
		}
		sessionTokenVal = strings.TrimSpace(string(body))
		if sessionTokenVal != "" {
			log.Printf("[agent] session token loaded from MMDS")
		}
	})
	return sessionTokenVal
}

// peakRssKb queries getrusage(RUSAGE_CHILDREN). On Linux ru_maxrss is in KB.
func peakRssKb() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &ru); err != nil {
		return 0
	}
	return ru.Maxrss
}

func truncate(s string) string {
	if len(s) <= maxOutputBytes {
		return s
	}
	return s[:maxOutputBytes]
}

// ---------- output buffer plumbing ------------------------------------------

type limitedBuffer struct {
	buf   strings.Builder
	limit int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	avail := l.limit - l.buf.Len()
	if avail <= 0 {
		return len(p), nil // silently drop, contest output cap honoured
	}
	if len(p) > avail {
		p = p[:avail]
	}
	n, _ := l.buf.Write(p)
	return n, nil
}

func (l *limitedBuffer) String() string { return l.buf.String() }

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// guard against an unused-import for fmt in stripped builds
var _ = fmt.Sprintf
