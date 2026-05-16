#!/usr/bin/env python3
"""
Publish a synthetic SubmissionEvent to the regional Kafka so the
execution-worker on oj-compute picks it up and runs it through the
Firecracker sandbox.

Usage (run from your Mac OR from inside the control-plane VM):

    # On the control-plane VM (where Kafka is local):
    sudo apt-get install -y python3-pip
    pip3 install kafka-python protobuf
    python3 submit-sample.py --code 'print(2+2)' --language python

    # With Java:
    python3 submit-sample.py --language java --code "$(cat <<'EOF'
public class Solution {
    public static void main(String[] args) {
        System.out.println(2 + 2);
    }
}
EOF
)"

The script does NOT touch CockroachDB. Workers will log a warning about a
missing submission row but the FC run + verdict publish still complete.

Wire-protocol details: SubmissionEvent is a protobuf with field tags 1-9;
we hand-roll the wire format here so the script has zero codegen
dependencies — no protoc, no generated Python modules to ship.
"""

import argparse
import base64
import os
import struct
import sys
import time
import uuid

# ---------- proto wire encoding ---------------------------------------------
# https://protobuf.dev/programming-guides/encoding/  — we only need:
#   wire type 0 (varint)         : (field << 3) | 0
#   wire type 2 (length-delim)   : (field << 3) | 2  + <varint len> + bytes


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _string_field(field: int, value: str) -> bytes:
    if value is None:
        value = ""
    payload = value.encode("utf-8")
    tag = (field << 3) | 2
    return _varint(tag) + _varint(len(payload)) + payload


def _int64_field(field: int, value: int) -> bytes:
    tag = (field << 3) | 0
    return _varint(tag) + _varint(value)


def build_submission_event(submission_id, user_id, problem_id, contest_id,
                            s3_code_url, language, gateway_ts_ms, region, phase):
    return (
        _string_field(1, submission_id)
        + _string_field(2, user_id)
        + _string_field(3, problem_id)
        + _string_field(4, contest_id)
        + _string_field(5, s3_code_url)
        + _string_field(6, language)
        + _int64_field(7, gateway_ts_ms)
        + _string_field(8, region)
        + _string_field(9, phase)
    )


# ---------- main ------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                    help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP or localhost:9092)")
    ap.add_argument("--topic", default="submissions.pretest",
                    help="Topic to publish to (default: submissions.pretest)")
    ap.add_argument("--language", default="python", choices=("python", "java", "cpp"),
                    help="Source language")
    src_group = ap.add_mutually_exclusive_group()
    src_group.add_argument("--code", default=None,
                           help="Contestant source as inline string. Embedded as data: URL.")
    src_group.add_argument("--code-file", default=None,
                           help="Path to a contestant source file. Embedded as data: URL.")
    ap.add_argument("--problem-id", default="00000000-0000-0000-0000-0000000cafee",
                    help="Problem UUID (default: the trivial print(2+2) smoke problem)")
    ap.add_argument("--input", default="",
                    help="(unused; stdin is now driven per-ordinal by problem-service test cases)")
    ap.add_argument("--region", default="asia-south1",
                    help="Region tag passed through to the verdict")
    args = ap.parse_args()

    # Resolve the contestant source: --code wins, then --code-file, else
    # a hardcoded fallback so the no-arg invocation still does something.
    if args.code_file:
        with open(args.code_file, "r", encoding="utf-8") as fh:
            source_code = fh.read()
    elif args.code is not None:
        source_code = args.code
    else:
        source_code = "print('hello firecracker')"

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Need `pip3 install kafka-python protobuf`", file=sys.stderr)
        sys.exit(1)

    submission_id = str(uuid.uuid4())
    user_id       = "00000000-0000-0000-0000-00000000beef"
    problem_id    = args.problem_id
    contest_id    = ""
    gateway_ts_ms = int(time.time() * 1000)
    phase         = "pretest"

    # data: URL — the worker's resolveSourceCode() handles this without
    # needing S3 / GCS to be wired up.
    encoded_code = base64.b64encode(source_code.encode("utf-8")).decode("ascii")
    s3_code_url  = f"data:text/plain;base64,{encoded_code}"

    payload = build_submission_event(
        submission_id, user_id, problem_id, contest_id,
        s3_code_url, args.language, gateway_ts_ms, args.region, phase,
    )

    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    future = producer.send(args.topic, value=payload, key=submission_id.encode("utf-8"))
    md = future.get(timeout=10)
    producer.flush()

    print(f"submission_id = {submission_id}")
    print(f"language      = {args.language}")
    print(f"topic         = {md.topic}  partition={md.partition}  offset={md.offset}")
    print()
    print("Watch the worker logs + the evaluated_results topic on the control-plane VM:")
    print("  docker exec -it oj-execution-worker tail -f /proc/1/fd/1   # on oj-compute")
    print("  docker exec -it oj-kafka kafka-console-consumer "
          "--bootstrap-server localhost:29092 --topic evaluated_results "
          f"--from-beginning --property print.key=true 2>/dev/null | grep {submission_id}")


if __name__ == "__main__":
    main()
