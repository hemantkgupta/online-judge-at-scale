# ADR-0006: vsock Go Bridge, Not JNI

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

The execution-worker is a JVM service running in a container on the host. Inside each Firecracker microVM, an in-guest Go agent listens on `AF_VSOCK` (the Linux socket family for host-guest VM communication). The worker must send the per-test execution request to that agent and read back the per-ordinal verdicts. The wire is vsock, not TCP.

The JVM has no native AF_VSOCK support. Three reasonable options exist for the integration:

1. JNI: write C glue + a Java loader.
2. Pure-Java vsock via `mdlayher/vsock` (Go library, not Java). Closest equivalent in JVM-land is `unix-jni` or similar libc-binding crates.
3. Shell out to a small Go binary that handles the vsock side and exchanges JSON over stdin/stdout with the JVM.

There's also a subtle wire-protocol gotcha: Firecracker's vsock implementation does not preserve AF_UNIX half-close semantics. If the worker does `shutdown(SHUT_WR)` after sending the request (the conventional way to signal "end of stream" to a vsock peer), the server side does not see EOF — it hangs waiting for more bytes. The protocol must use length-prefixing or message framing, not half-close.

## Decision

Use option 3: shell out to `oj-vsock-client`, a tiny (~250-line) statically-linked Go binary baked into the worker container image. The JVM exchanges JSON over the child's stdin/stdout. The Go binary owns the vsock side and the message framing.

```java
ProcessBuilder pb = new ProcessBuilder("/usr/local/bin/oj-vsock-client", vsockUdsPath, String.valueOf(port));
Process p = pb.start();
p.getOutputStream().write(requestJson);
p.getOutputStream().close();   // close stdin — NOT shutdown of the vsock
String response = readAllFrom(p.getInputStream());
p.waitFor();
```

## Alternatives considered

**JNI from the JVM directly to AF_VSOCK.** Adds a libc dependency to the worker container image. The worker is currently built on the slim JVM base image with no libc; adding it expands the image, the CVE surface, and the build complexity. The JNI layer also has to handle the half-close gotcha itself.

**Pure-Java/Kotlin vsock library.** None exists in production-grade form for the JVM (as of 2026). Writing one is a substantial maintenance burden for ~250 lines of behaviour.

**HTTP-over-vsock.** Treat the vsock as a transport, run an HTTP server inside the microVM. Adds latency (HTTP framing + parsing), adds dependencies, and doesn't actually solve the half-close problem (because HTTP/1 keep-alive needs proper Connection: close handling anyway).

**Worker rewrite in Go.** Would eliminate the JVM-to-vsock gap entirely. Considered briefly and rejected — the rest of the worker's surface (Kafka consumer, JPA, Spring observability, Prometheus integration, idempotency state machine) is significantly more ergonomic in the JVM than in Go.

## Consequences

**Positive:**
- Statically-linked Go binary has no libc dep; the worker image stays slim.
- The half-close gotcha is fully encapsulated in the Go binary; the JVM never has to know.
- Easy to test the Go binary in isolation (it's a CLI; pipe JSON in, read JSON out).
- Process boundary doubles as a fault boundary: a bug in the vsock library can't crash the JVM.

**Negative:**
- One extra process spawn per execution. The wall-clock cost is ~5-10 ms — small relative to the ~150 ms VM boot and the contestant code execution.
- The Go and Java sides must agree on the JSON schema for the request/response. Versioning is informal.
- Releasing the worker image requires shipping both a JAR and the Go binary together.

## Implementation pointers

- The bridge source: `infra/firecracker/vsock-client/main.go` (a CLI: argv[1] = vsock UDS path, argv[2] = port).
- The Dockerfile that bakes the bridge into the worker image: `execution-worker/Dockerfile`.
- The JVM-side caller: `execution-worker/.../service/AgentClient.java`.
- The half-close discipline: documented at [`services/sandbox-manager.md#38-vsock-bridge`](../services/sandbox-manager.md).

## Related

- [`services/execution-worker.md`](../services/execution-worker.md) §3.6
- [`services/sandbox-manager.md`](../services/sandbox-manager.md)
- [`flows/submission-roundtrip.md`](../flows/submission-roundtrip.md) §3 step 10
