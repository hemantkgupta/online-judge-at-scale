# Documentation Index

> **Canonical design source:** [`online-judge-at-scale.md`](../../../CSE-Raw/raw-blog/online-judge-at-scale.md) — the full system-design write-up. *(Local path during development; will be replaced by the published Substack URL when the post goes live.)*

This folder contains the supporting documentation for the companion code in this repo. The **blog** is where the architecture is explained from first principles; the docs here describe *what's actually built* and how the code lines up against the design.

If you're new to the repo, start with the root [`README.md`](../README.md) for setup and module orientation, then come back here.

## What's in this folder

| File | Purpose | Read this when… |
|---|---|---|
| [`code-companion.md`](code-companion.md) | File-level map from each blog Part → the Java source files that implement it; gaps table noting what's still missing locally. | You're reading a section of the blog and want to find the exact file that implements it. |
| [`research-checkpoint.md`](research-checkpoint.md) | Design rationale — why each foundational mechanism exists (transactional outbox, event-time scoring, gateway timestamping, etc.). | You want the *why* behind a design choice before diving into code. |
| [`implementation-plan.md`](implementation-plan.md) | The original phase-by-phase implementation plan; now largely executed. Per-phase code maps are listed where they exist. | You want to understand how the codebase was built up in phases. |
| [`parity-plan.md`](parity-plan.md) | Historical: the plan that brought the repo to parity with a sister project's quality bar. Status header at the top says what's done. | You're curious about the project's development history. |

## How the docs and the blog stay in sync

The **sync rule** (declared in `code-companion.md`): *if the blog claims a mechanism exists, the companion must point to the file or test that implements it.* If no file exists, the entry says so, and the **Gaps** section (also in `code-companion.md`, mirrored in the blog's final **Gaps vs Production** table) names what's intentionally not implemented locally.

Two failure modes the sync rule protects against:
1. Blog claims a feature that doesn't exist in code (false advertising).
2. Code has a feature the blog never mentions (orphaned implementation).

Both `code-companion.md` and the blog's gap table get updated together when the code changes.

## Running the code

See the root [`README.md`](../README.md) — `docker compose up -d` + `./gradlew test` gets you from clone to a green test suite without needing the blog open.
