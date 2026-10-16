# Pi Integration Development Plan

Status: planning baseline (not started)
Last updated: 2026-10-15
Related: [A2A Protocol Plugin Development Plan](DEVELOPMENT_PLAN.md),
[Goose Integration Development Plan](GOOSE_INTEGRATION_PLAN.md)

## Goal

Let A2A tasks handled by `a2a_protocol_plugin` be served by
[pi](https://github.com/earendil-works/pi) (pi.dev), a minimal, self-extensible
terminal coding agent with a multi-provider LLM API, the same way goose,
Hermes and OpenClaw are bridged. The bridge is `PiAgentHandler`
(`agent_type: "pi"`), deployed as a `pi` compose profile in
`docker-a2a-gateway`.

```
A2A client ──▶ silvaengine_gateway ──▶ silvaengine_daemon ──▶ a2a_protocol_plugin
                                                                 │ PiAgentHandler
                                                                 │ (PiSidecarClient, HTTP + SSE)
                                                                 ▼
                                      pi sidecar (Node)  ── stdin/stdout JSONL ──▶ pi --mode rpc
                                      (container: pi)        one process per active session
```

## Why a sidecar

pi has **no network server**. Its integration surfaces are:

| Interface | What it is | Decision |
| --- | --- | --- |
| **RPC mode** (`pi --mode rpc`) | Official. Long-lived subprocess; JSONL commands on stdin, responses and events on stdout. | **Used**, behind a sidecar. |
| SDK (TypeScript) | Official. Runs pi in-process in Node/Bun; ships an `RpcClient` for subprocess control. | Used **inside the sidecar** to drive RPC mode. |
| `pi-acp` (community) | ACP over **stdio**, wrapping pi. MCP servers are accepted but not wired through. | Not used: our ACP client (goose) speaks ACP over HTTP, and an extra community layer adds risk. |
| `--mode json`, `-p` | One-shot runs. | Not used: no cancel or follow-up turns. |

Options considered for putting pi on the network:

| Option | Verdict |
| --- | --- |
| **A. HTTP sidecar in its own container** (Node service driving `pi --mode rpc`) | **Chosen.** Same shape as the goose sibling: pi's shell/file tools stay out of the gateway, the gateway stays serverless-friendly, sessions live on a volume. |
| B. Handler spawns `pi --mode rpc` inside the gateway | Rejected: needs Node in the gateway image, runs pi's tools inside the gateway, breaks under serverless dispatch. Acceptable only for the spike. |
| C. Sidecar fakes an OpenAI-compatible API; reuse `OpenClawAgentHandler` | Rejected: loses sessions, cancel, tool events and approvals. |

## pi RPC facts the design relies on

From the official docs (`rpc.md`, `rpc-commands.md`, `json.md`,
`rpc-extension-ui.md`, `cli.md`). Items marked **(verify)** are not stated in
the docs and are confirmed in the spike (P1).

- **Start:** `pi --mode rpc` with normal CLI options: `--provider` + `--model`,
  `--thinking <level>`, `--session <path|id>` / `--session-id <id>` /
  `--session-dir <dir>` / `--no-session`, `--tools` / `--no-tools`,
  `--no-extensions`, `--system-prompt` / `--append-system-prompt`,
  `--approve` / `--no-approve` (project trust), `--offline`.
- **Framing:** strict JSONL, split on LF only (not Node `readline`, which also
  splits on U+2028/U+2029). Stdout carries protocol records only;
  diagnostics go to stderr. Closing stdin shuts pi down cleanly.
- **Commands** carry an optional `id`; the `response`
  (`{type, command, success, data | error, id}`) echoes it and may arrive out
  of order. A malformed record yields `command: "parse"`.
  - `prompt` (`message`, `images?`, `streamingBehavior?` = `steer` |
    `followUp`) → `data.disposition` = `started` | `queued` | `handled`
    (`handled` means no run starts and no `agent_settled` follows).
  - While a run is streaming, a `prompt` **without** `streamingBehavior`
    fails; `steer` is delivered before the next LLM call, `followUp` when the
    agent stops.
  - `abort`, `steer`, `follow_up`, `clear_queue`.
  - Sessions: `new_session`, `switch_session` (`sessionPath`), `fork`,
    `clone`, `get_state` (→ `sessionId`, `sessionFile`, `isStreaming`,
    `model`, `thinkingLevel`, …), `get_messages`, `get_session_stats`
    (tokens, cost, context usage).
  - Model: `set_model` (`provider`, `modelId`), `set_thinking_level`
    (`off` … `max`), `get_available_models`.
- **Events** (no command `id`):
  - `agent_start`, `turn_start`, `turn_end`, `agent_end` (`willRetry`),
    `agent_settled` (`aborted`) — settled means pi will not continue on its
    own.
  - `message_start`, `message_update` (`usage`, `assistantMessageEvent`),
    `message_end` (authoritative final message).
  - `assistantMessageEvent` subtypes: `text_start/_delta/_end`,
    `thinking_start/_delta/_end`, `toolcall_start/_delta/_end`, `done`,
    `error` (`reason`, `error`).
  - `tool_execution_start/_update/_end` (`toolCallId`, `toolName`, `args`,
    `result`, `isError`, `durationMs`).
  - `auto_retry_start/_end`, `compaction_start/_end`, `queue_update`,
    `extension_error`.
- **Extension UI:** `extension_ui_request` with `method` = `select` |
  `confirm` | `input` | `editor` (dialogs, block until answered; optional
  `timeout` ms) or `notify` | `setStatus` | `setWidget` | `setTitle` |
  `set_editor_text` (fire-and-forget). Answer with `extension_ui_response`
  (`id`, and `value` | `confirmed` | `cancelled`).
- **Permissions:** pi has **no approval prompts for built-in tools** —
  "Enabled tools use the operating-system permissions of the Pi process".
  Only extensions ask, via extension UI dialogs. The container is the
  security boundary.
- **Sessions** are JSONL tree files; the default directory is under
  `~/.pi/agent` **(verify)**; `--session-dir` / `PI_CODING_AGENT_SESSION_DIR`
  override it. Config lives in `~/.pi/agent` (`settings.json`, `mcp.json`).
- **Providers:** `--provider` / `--model` / `--api-key`; per-provider
  credential env vars and custom OpenAI-compatible endpoints **(verify)** in
  the Providers doc.

## Streaming and reasoning: follow the existing bridges

pi must stream and render reasoning exactly like goose, Hermes, OpenClaw and
the core engine already do. It adds **no new streaming path**: the handler
feeds the shared bridge in `a2a_ai_agent_utility.py`
(`execute_ai_agent_streaming`), which owns SSE emission, reasoning handling,
the `show_reasoning` switch, persistence and the final reply. The rules below
are what the other bridges already follow; pi is reviewed and tested against
them.

### The shared contract

| Concern | Rule (already implemented in the bridge) | pi handler obligation |
| --- | --- | --- |
| Stream chunks | Handler pushes `{"name", "value", "meta"}` to `stream_queue`; names `run_id`, `token`, `tool_call`, `tool_result`, `approval`, `auth_required`, `error`; sets `stream_event` in `finally`. | Same names only; set `stream_event` in `finally`. |
| Answer tokens | `token` without reasoning meta → appended to the answer and streamed to SSE as a `task_artifact` (`artifact.text`). | `text_delta` → `token`. |
| Reasoning tokens | `token` with `meta.data_format = "reasoning"` (or an `rs#` marker) is detected by `_is_reasoning_meta`, **kept out of the answer**, streamed as a tagged artifact (`artifact.metadata.data_format = "reasoning"`) and returned as `metadata.reasoning`. | `thinking_delta` → `token` + `{"data_format": "reasoning"}`. |
| Reasoning switch | `resolve_show_reasoning`: request `metadata.show_reasoning` > agent metadata `show_reasoning` > `A2A_SHOW_REASONING` (default on). Off → reasoning neither streamed nor returned. | Nothing — always emit tagged reasoning; the bridge applies the switch. Optionally pass `pi_thinking` to pi so the model reasons at all. |
| No duplicate answer | Never tag a copy of the answer as reasoning (the Hermes `reasoning.available` bug, fixed 2026-10-15). | Use only `thinking_delta`; ignore `thinking_end` / `message_end` full-content copies, which repeat what was already streamed. |
| Replay | History replayed by a session load must never become tokens (goose `session/load` epoch filter). | Only forward events of the current run; `switch_session` must not emit old messages as deltas **(verify in P1)**. |
| SDK reply | Exactly one accumulated agent Message to the SDK queue after the stream (SDK v2 rejects more); status events go to SSE only. | Nothing — handled by the bridge. |
| SSE scoping | Artifacts and status are scoped by the client's per-turn `task_id` (`sse_task_id`), then `COMPLETED` / `FAILED` status. | Nothing. |
| HTTP fallback | The JSON-RPC response carries the full answer even when streaming, for clients that miss SSE frames. | Return the aggregated answer (without reasoning) as `content`. |
| Cancel / approval | `run_id` chunk registers the run (`_on_external_run_id`) for `cancel_run` / `resolve_approval`; `approval` → `INPUT_REQUIRED`. | Push `run_id` first; `approval` for `ask`-mode dialogs. |

### How each bridge sources reasoning today

| Bridge | Native reasoning event | Mapped to |
| --- | --- | --- |
| goose | ACP `session/update` `agent_thought_chunk` | tagged `token` |
| Hermes | `reasoning.delta`; `reasoning.available` only when it differs from the answer | tagged `token` |
| OpenClaw | `delta.reasoning_content` / `delta.reasoning` (none sent today) | tagged `token` |
| core engine | stream frames carrying an `rs#` marker in `message_group_id` | detected by `_is_reasoning_meta` |
| **pi** | `message_update` → `assistantMessageEvent.thinking_delta` | tagged `token` |

### Test scripts follow the same layout

The `tests/pi/*` scripts reuse `a2a_test_utils.StreamSections` and
`is_reasoning_artifact`, so pi output looks like every other bridge (and like
`silvaengine_gateway`'s `chat_websocket.py`):

```
>>> REASONING START      (dimmed thinking)
<<< REASONING END
>>> RESPONSE START
<<< RESPONSE END
```

`--hide-reasoning` sends `metadata.show_reasoning=false`. `test_sse_live.py`
checks, as for goose: health, SSE connect, live answer tokens, `COMPLETED`,
HTTP fallback carries the reply, and a direct sidecar streaming check — plus
a reasoning check: with a reasoning model, tagged reasoning arrives and is
absent from the answer; with `--hide-reasoning`, none arrives.

## Design

### 1. Sidecar service (`docker-a2a-gateway/pi/`)

A small Node service (TypeScript) using pi's exported `RpcClient` to manage
`pi --mode rpc` child processes. It owns the stdio protocol; the plugin only
speaks HTTP + SSE.

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Liveness + pi version. |
| `POST /sessions` | Start a session → `{sessionId, sessionFile}` (`new_session`). Body: `cwd?`, `model?`, `thinking?`. |
| `POST /sessions/{sessionId}/prompt` | Send a prompt; response is `text/event-stream` of pi events for this run until `agent_settled`. Body: `message`, `images?`, `streamingBehavior?`. Loads the session (`switch_session`) if no process is holding it. |
| `POST /sessions/{sessionId}/abort` | `abort` the running prompt. |
| `POST /sessions/{sessionId}/ui-response` | Forward an `extension_ui_response` (`id`, `value` / `confirmed` / `cancelled`). |
| `GET /sessions/{sessionId}` | `get_state` + `get_session_stats`. |
| `DELETE /sessions/{sessionId}` | Close the process (session file stays on disk). |

- **Process pool:** one pi process per active session; idle processes are
  closed after `PI_IDLE_TIMEOUT` (stdin close → clean shutdown); a cap
  (`PI_MAX_PROCESSES`) protects the container. A session's file survives
  process exit, so the next prompt re-opens it with `switch_session`.
- **One run per session:** a second prompt while one is streaming is
  rejected with HTTP 409 unless it sets `streamingBehavior`.
- **SSE frames:** pi events forwarded as-is (`data: <pi event JSON>`), plus a
  final `data: {"type":"run_end","aborted":…,"stopReason":…,"usage":…}`.
- **Auth:** shared secret `PI_SIDECAR_SECRET_KEY` in an `X-Secret-Key`
  header (same pattern as goose); 401 otherwise. Bind 0.0.0.0 inside the
  compose network only.
- **Sessions on a volume:** `--session-dir /data/sessions`; working directory
  `/workspace` (bind-mounted, like goose).

### 2. Plugin transport — `handlers/pi_client.py` (`PiSidecarClient`)

Synchronous `httpx` with an `http_transport` injection point for tests, same
as `goose_acp_client.py`. Owns HTTP/SSE details only: create session, stream
a prompt, abort, ui-response, close. A change in the sidecar API touches only
this file.

### 3. Plugin handler — `handlers/pi_handler.py` (`PiAgentHandler`)

- **Conversation key → pi session, per tenant.** The conversation key is the
  explicit `thread_uuid`, else the A2A `context_id`
  (`context["thread_uuid"] or context["context_id"]`). It is mapped to a pi
  `sessionId` through `conversation_map.py`
  (`setting_id = "pi_session#{key}"`) under the request's `partition_key`.
  A client-supplied id is never passed to the sidecar unverified; an unknown
  key or a missing session starts a new one.
- **Only the new user turn is sent** — pi keeps the history in its session
  file (like goose).
- **Event → stream chunk mapping** (per the shared contract in
  [Streaming and reasoning](#streaming-and-reasoning-follow-the-existing-bridges)):

  | pi event | Bridge chunk |
  | --- | --- |
  | `message_update` → `text_delta` | `token` (answer; collected into `content`) |
  | `message_update` → `thinking_delta` | `token` with `meta.data_format = "reasoning"` (follows `show_reasoning`) |
  | `tool_execution_start` | `tool_call` (`toolName`, `args`, `toolCallId`) |
  | `tool_execution_end` | `tool_result` (`result`, `isError`) |
  | `message_update` → `error` / failed `message_end` | `error` |
  | `extension_ui_request` dialog (`confirm`/`select`/`input`/`editor`) | permission policy (below) |
  | `auto_retry_*`, `compaction_*`, `queue_update`, fire-and-forget UI | ignored (logged) |
  | `agent_settled` / `run_end` | end of turn; `aborted` → canceled |

  The final `message_end` usage goes to `metadata.usage`.
- **Run id** for cancel/approval: `"{sessionId}:{runSeq}"`, pushed first as a
  `run_id` chunk. `cancel_run()` → `POST …/abort`; the run settles with
  `aborted: true` and the task ends CANCELED.
- **Second message during a run:** sent with `streamingBehavior: "followUp"`
  (queued until the agent stops) rather than failing; configurable
  (`pi_busy_behavior` = `followUp` | `steer` | `reject`).
- **Extension dialogs** (`pi_permission_mode`, mirrors goose):
  - `auto_allow` (default): `confirm` → `confirmed: true`; `select` → first
    option; `input`/`editor` → `cancelled`.
  - `deny`: every dialog → `cancelled` (confirm → false).
  - `ask`: emit an `approval` chunk (executor → `INPUT_REQUIRED`); wait for
    `resolve_approval()` or `pi_approval_timeout`, then answer. Daemon mode
    only (same limit as goose).
- **Non-streaming:** same flow with an internal queue; return the aggregated
  `{content, role, metadata}`.
- **Configuration** (agent metadata → setting → `os.environ` → `Config` →
  default; the secret prefers setting/env over metadata):

  | Metadata key | Setting / env | Default |
  | --- | --- | --- |
  | `pi_api_url` | `PI_API_URL` | `http://localhost:3300` |
  | `pi_secret_key` (fallback only) | `PI_SECRET_KEY` (= sidecar `PI_SIDECAR_SECRET_KEY`) | — |
  | `pi_cwd` | `PI_CWD` | `/workspace` |
  | `pi_model` | `PI_MODEL` (`provider/modelId`) | sidecar default |
  | `pi_thinking` | `PI_THINKING` (`off` … `max`) | sidecar default |
  | `pi_permission_mode` | `PI_PERMISSION_MODE` | `auto_allow` |
  | `pi_busy_behavior` | `PI_BUSY_BEHAVIOR` | `followUp` |
  | `pi_timeout` | `PI_STREAM_TIMEOUT` | `300` |
  | `pi_approval_timeout` | `PI_APPROVAL_TIMEOUT` | `120` |

- **Errors:** 401/403 → task FAILED naming `PI_SECRET_KEY`; 404 session →
  new session; 409 busy → per `pi_busy_behavior`; sidecar unreachable or
  stream dropped → FAILED with partial content.

### 4. Registration

`AGENT_TYPE_MAP["pi"] = {module_name: "a2a_protocol_plugin.handlers.pi_handler",
class_name: "PiAgentHandler"}`; `pi_*` attributes and `_set_parameters`
wiring in `handlers/config.py` (mirror `goose_*`).

## Phases

### P1: Spike

- Build a throwaway container with pi installed; drive `pi --mode rpc`
  directly and capture a real exchange into
  `tests/pi_rpc_transcript.json`: plain answer, thinking, a tool call
  (`bash`/`write`), `abort` mid-run, a prompt while streaming
  (with/without `streamingBehavior`), `switch_session` resume, and an
  extension `confirm` dialog.
- Settle the **(verify)** items: npm package name and Node version, default
  session dir and session id format, provider env vars, custom
  OpenAI-compatible endpoint (to reuse `OPENAI_COMPAT_*`), behaviour of an
  unanswered dialog without `timeout`, project-trust prompt in headless mode
  (`--approve`).

### P2: Sidecar

- `docker-a2a-gateway/pi/`: Node service (`RpcClient` pool, HTTP + SSE,
  secret, idle reaping, process cap), Dockerfile pinning the pi version,
  supervisord program, healthcheck script (`GET /health` with the secret),
  first-start provider config from `.env`.
- Unit tests for the sidecar's JSONL framing (LF-only split, U+2028 in
  content) and pool lifecycle.

### P3: Plugin handler

- `handlers/pi_client.py`, `handlers/pi_handler.py`, config wiring,
  `AGENT_TYPE_MAP["pi"]`, session mapping through `conversation_map.py`.

### P4: Tests

- `tests/test_pi_handler.py` (mocked sidecar from the P1 transcript):
  - [ ] first turn and resumed turn (mapping, never a client-supplied id)
  - [ ] cross-tenant key starts a new session
  - [ ] answer vs tagged reasoning (`thinking_delta` only; `thinking_end` /
        `message_end` copies never re-emitted)
  - [ ] through `execute_ai_agent_streaming`: reasoning kept out of the
        answer, streamed as a tagged artifact, returned as
        `metadata.reasoning`; `show_reasoning` off drops it (same assertions as
        `TestStreamingShowReasoning`)
  - [ ] resumed session emits no replayed history as tokens
  - [ ] tool call / result chunks
  - [ ] abort → canceled; error event → FAILED
  - [ ] busy session → followUp / steer / reject
  - [ ] each permission mode for each dialog type, and the `ask` timeout
  - [ ] 401 / 404 / 409 / connection-drop paths
- Executor-level test through `A2ADaemonEngine.a2a()` (like
  `test_goose_executor.py`): send, echoed-`contextId` resume, tenant
  isolation.
- `tests/test_pi_live.py` gated on `A2A_RUN_LIVE_PI_TESTS=1`;
  `tests/register_pi_agent.py`.

### P5: Deployment (`docker-a2a-gateway`, `pi` profile)

- `pi` compose service (port `PI_PORT` 3300, volumes for sessions/config and
  `./www/pi-workspace` → `/workspace`), gateway `depends_on` (optional),
  seeded `a2a-pi-agent`, `.env.example` section, `generate_env.py`
  generating `PI_SIDECAR_SECRET_KEY` = `PI_SECRET_KEY`.
- `tests/pi/*` five-script set (`test_hello`, `test_hello_sse`,
  `test_gateway_live`, `test_sse_live`, `test_chatbot`) copied from the goose
  set: `StreamSections` tagged reasoning sections, `--hide-reasoning`, and the
  reasoning check in `test_sse_live` (see
  [Test scripts follow the same layout](#test-scripts-follow-the-same-layout));
  `make pi-up` / `pi-down` / `pi-test` / `pi-chat`.
- README: add pi to the bridge table in the "Reasoning" section
  (`thinking_delta`) and to the bridge / sibling tables.

### P6: Docs

- `DEVELOPMENT_PLAN.md` Phase 5 agent list, `AGENTS.md` layout, README
  (bridge table, sibling table, Reasoning table row, security notes, known
  limitations, troubleshooting).

## Acceptance Criteria

- [ ] `message/send`, `message/stream`, `tasks/get` and `tasks/cancel` work
      end to end against a live pi sidecar.
- [ ] Echoing the returned `contextId` (or resending `metadata.thread_uuid`)
      resumes the same pi session; another tenant's key never does.
- [ ] Streaming follows the shared contract: answer tokens and tagged
      reasoning are separate SSE artifacts, the reply carries
      `metadata.reasoning`, the HTTP response carries the answer only, and
      `show_reasoning=false` removes reasoning — verified with the same
      scripts and assertions as goose / Hermes / OpenClaw.
- [ ] `tests/pi/*` render `>>> REASONING` / `>>> RESPONSE` sections like the
      other bridges.
- [ ] `tasks/cancel` aborts the pi run (CANCELED).
- [ ] Extension dialogs are handled per `pi_permission_mode`; `ask`
      produces `INPUT_REQUIRED` and resumes on approval in daemon mode.
- [ ] Unit tests run without pi; live suites pass against the pinned pi
      version in the docker stack.

## Risks

| Risk | Mitigation |
| --- | --- |
| No network server; we own the sidecar. | Keep it small and stateless apart from the process pool; drive pi through its own `RpcClient`; unit-test framing and lifecycle. |
| Built-in tools run without approval. | Own container, workspace volume only, no host mounts, required secret; document that `pi_permission_mode` only covers extension dialogs; optionally restrict tools (`--tools` / `--exclude-tools`) per agent. |
| One process per active session can exhaust memory. | `PI_MAX_PROCESSES` cap, idle reaping, 503 with a clear error when full. |
| Session files grow without bound. | pi auto-compaction stays on; document volume cleanup. |
| Prompt while streaming is rejected by pi. | Default `followUp`; configurable `steer` / `reject`. |
| pi's reasoning or streaming diverges from the other bridges (duplicated answer as reasoning, replayed history, a second SDK Message). | Handler emits only through the shared chunk contract; tests reuse the existing streaming/reasoning assertions and the shared test-script helpers. |
| pi RPC protocol changes between versions. | Pin the pi version in the sidecar image; replay the P1 transcript in tests; re-run live suites on upgrade. |
| Extension dialog with no `timeout` and no answer could hang a run. | Policy always answers dialogs; `ask` has `pi_approval_timeout`; sidecar aborts a run past `PI_STREAM_TIMEOUT`. |

## References

- pi RPC mode: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md
- pi RPC commands: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc-commands.md
- pi JSON event stream: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/json.md
- pi RPC extension UI: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc-extension-ui.md
- pi CLI reference: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/cli.md
- How pi works: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/how-pi-works.md
- pi-acp (community ACP adapter): https://github.com/victor-software-house/pi-acp
