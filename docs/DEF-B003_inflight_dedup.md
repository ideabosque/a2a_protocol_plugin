# DEF-B003 — In-flight duplicate submission: dedup-observe only attaches after the first invocation completes

**Status**: fixed and verified locally, 2026-11-02; uncommitted and not remotely deployed. **Severity**: High before correction.

Current evidence: [local verification report](https://github.com/banyanos/orchestration_engine/blob/main/docs/INTEGRATION_FIX_VERIFICATION_20261102.md). Atomic public reservations precede dispatch; live concurrent submissions and terminal replay returned the same task, while a new message ID created a distinct turn. Read-only reconciliation found exactly two backend tasks for those two intended turns. Long execution and cancellation also passed. PostgreSQL was tested live; DynamoDB conditional claims were reviewed without live AWS validation. Historical observations and the earlier proposed design remain below.
**Discovered**: 2026-11-01, live in-browser E2E on `docker-banyanos-gateway` (Phase 4 checkpoint-worker validation round).
**Plan reference**: `banyanos/orchestration_engine/docs/a2a_integration_development_plan.md` — §"Invocation, scheduling and recovery contract" / §"Retry, security, and event rules" ("After an ambiguous timeout, reconcile existing remote work before resubmitting side-effecting tasks").

## 1. Observed behavior (E2E evidence)

One orchestration REMOTE node (`a2a-suspend-e2e` → published agent `banyan-hospi-slow-agent`, message_id `orch-{trace}-a2a_gemini`) whose backend agent ran a 30-item tool traversal (inline phase ≫ 30s):

| attempt | backend agent task | started | finished | outcome |
|---|---|---|---|---|
| 1 | `7b34d6ee` | 04:47:46Z | 04:48:55Z | FAILED (HTTP 429, provider outage) |
| 2 | `e559f2f2` | 04:48:17Z | 04:49:37Z | FAILED (HTTP 429) |
| 3 | `18594f47` | 04:48:49Z | (still RUNNING after the node FAILED at 04:49:19) | orphan |

Three `SendMessage` invocations for **one** logical message. The orchestration node never suspended (see DEF-ORB-001): each attempt ended in `OrchestrationA2aCallError` (transport timeout) and the retry loop re-dispatched immediately. Plugin A2A task rows: 1724e654 / edd33db3 / 8ee7c408 — three separate FAILED tasks, one per attempt.

**Additional evidence (2026-11-01 05:29Z, failure-contract run, trace `f64dd746`)**: a REMOTE node referencing a *nonexistent* publication was retried by the orchestration (2 attempts visible: plugin tasks `c6a730b2-…` at 05:29:26Z and `28895f8e-…` at 05:29:28Z, both FAILED) — the retried attempts received **fresh A2A context ids each time** (`0a99bcbc…` vs `0d7cc415…`). The stable logical message id therefore did not converge onto the same A2A task/context even for a terminal-failure replay: the dedup/observe-replay path (9816a31) never attached across these attempts. Scope extension of this defect: **dedup must converge both in-flight submissions and terminal replays (success AND failure) onto the first task/context reservation**.

## 2. Root cause (function-level)

- `handlers/banyanos_agent_handler.py` — synchronous SendMessage path calls `client.send_message(...)` **blocking to terminal**, then records the execution map (`record_execution`) — so `backend_ref.task_id` exists in durable storage **only after** the invocation returns.
- The 9816a31 dedup branch keys on `reservation.get("deduplicated") and (reservation.get("backend_ref") or {}).get("task_id")`. During the first (in-flight) invocation the map row does not exist yet, so the retried reservation resolves with **no backend task reference** → the handler falls through to a fresh `SendMessage` — a repeat of a side-effecting operation after an ambiguous (timeout) response, which the plan's retry rules explicitly forbid.
- The orchestration executor treats the transport timeout as a retryable `OrchestrationA2aCallError` (max_node_retry = 3 in `orchestration_default_execution`), so the duplicate window is exercised on every slow backend.

## 3. Fix design (proposal)

Core principle (already in the plan): **reserve before dispatch; the reservation must be visible to duplicate submissions even while the first invocation is in flight.**

1. **Plugin — write the execution-map row before the backend submission** (`banyanos_execution_map.py` + handler sync path):
   - At reservation/submit time, persist the map row with `state = "WORKING"`, an empty `backend_ref.task_id`, the logical `message_id`/idempotency key, and `submitted_at`.
   - After `send_message` returns (success **or** error), update the row: final `state`, real `backend_ref.task_id`, timestamps.
   - **Dedup branch change**: a reservation whose row exists with `state == WORKING` (no terminal `backend_ref`) must take the **observe-only** path: return the continuation signature (`banyanos_continuation: "queued"`, status WORKING) — never re-invoke. The executor keeps the A2A task WORKING; the plugin's continuation poller (`banyanos_continuation_poller.py`) reconciles the original invocation and replays the stored reply via `tasks/get` (deba8f9).
   - Terminal reservation replays (current 9816a31 behavior) remain unchanged.
2. **Orchestration — keep, do not expand, the retry**: with (1) in place the retry's re-submission is idempotent (observe-only), and the pending/WORKING response drives the node into the WAITING/suspension path (8034f6b) instead of failing. Optionally annotate the CallError as "observation timeout" for the audit trail (see DEF-ORB-001).

## 4. Acceptance criteria

- Repeated logical submission while the first backend invocation is in flight → **exactly one** backend agent task for the message_id (assert via `tenant_agent_task` count per trace in the E2E).
- Retry after terminal → replay of the recorded result (existing behavior preserved; 9816a31 tests stay green).
- The suspended execution completes through the suspension worker (`tasks/get` → replay → `execute_dag(resume_node_states=…)` → SUCCESS) without a duplicate backend invocation.
- New unit tests: `tests/test_banyanos_agent_handler.py` (in-flight dedup observe, observe-failure stays WORKING) + `tests/test_banyanos_execution_map.py` (reservation-time row with pending state). Existing 25 tests keep passing.

## 5. Related

- DEF-B002 (continuation → WORKING Task) supplies the nonterminal plumbing this fix relies on.
- DEF-ORB-001 (orchestration side): the 30s cap is what makes the in-flight window reachable; fixing DEF-ORB-001 shifts most slow-execution flows onto the accept-then-observe pattern and shrinks this window for synchronous-only targets.
