// Package main implements oj-vsock-client, a tiny host-side helper that
// lets the Sandbox Manager (Java) talk to the Execution Agent (Go, in-VM)
// over Firecracker's vsock without needing AF_VSOCK support in the JVM.
//
// Firecracker exposes vsock as a Unix-domain socket on the host. The host
// connects to that UDS, sends "CONNECT <guest_port>\n", reads back "OK
// <local_port>\n", and from there it's a bidirectional byte pipe to the
// guest's vsock listener. We wrap that handshake so Java can just
// ProcessBuilder this binary, write JSON to stdin, and read JSON from
// stdout — exactly like piping `nc` to a TCP service.
//
// Usage:
//
//	oj-vsock-client <uds-path> <guest-port>
//
// stdin → vsock; vsock → stdout. Exits 0 on EOF from either side.
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: oj-vsock-client <uds-path> <guest-port>")
		os.Exit(2)
	}
	udsPath, port := os.Args[1], os.Args[2]

	conn, err := net.Dial("unix", udsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", udsPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Firecracker vsock CONNECT handshake.
	if _, err := fmt.Fprintf(conn, "CONNECT %s\n", port); err != nil {
		fmt.Fprintf(os.Stderr, "send CONNECT: %v\n", err)
		os.Exit(1)
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "read CONNECT response: %v\n", err)
		os.Exit(1)
	}
	if !strings.HasPrefix(line, "OK") {
		fmt.Fprintf(os.Stderr, "vsock CONNECT failed: %s", line)
		os.Exit(1)
	}

	// Bidirectional pipe.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// stdin → vsock
		//
		// Deliberately do NOT call (*net.UnixConn).CloseWrite() after stdin
		// EOF. On a plain AF_UNIX stream socket a half-close is the correct
		// way to signal "no more input," but Firecracker's vsock layer does
		// not preserve half-close semantics — it treats CloseWrite on the
		// host UDS as a FULL teardown of the vsock connection. The agent
		// then fails its response write with "broken pipe" and we lose the
		// reply. Instead we just let this goroutine return; the agent
		// closes its end after writing the response, which propagates back
		// as a regular EOF on the vsock → stdout copy below.
		_, _ = io.Copy(conn, os.Stdin)
	}()
	go func() {
		defer wg.Done()
		// vsock → stdout (replaying anything buffered in br first)
		_, _ = io.Copy(os.Stdout, br)
	}()
	wg.Wait()
}
