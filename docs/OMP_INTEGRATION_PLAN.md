# omp (oh-my-pi) Integration Development Plan

Status: IMPLEMENTED on branch `omp-integration` — the plugin side (shared
`SidecarAgentHandler` base, the pi and omp dialects, config, registration,
tests) and the sidecar's omp dialect are in place. The O1 spike ran on
2026-11-10 against omp 18.18.2, and the same day the live checks ran against
a real omp sidecar container: the sidecar's HTTP + SSE surface (session
create, prompt → `run_end`, provider failure, abort, the native approval
dialog, the busy/follow-up path) and the plugin's gated live suite
(`tests/test_omp_live.py`, 5 passed / 1 skipped) plus an A2A `message/send`
round trip through the real executor. **Still open:** the gateway-level end
to end (`tests/omp/*` through the `a2a-gateway` container) — the local compose
stack could not be recreated while its `compose.projects.local.yaml` include
chain references a missing file.
Last updated: 2026-11-10
Branch: `omp-integration` (a2a_protocol_plugin + docker-a2a-gateway)
Related: [Pi Integration Development Plan](PI_INTEGRATION_PLAN.md),
[Goose Integration Development Plan](GOOSE_INTEGRATION_PLAN.md),
[BanyanOS Integration Plan](BANYANOS_INTEGRATION_PLAN.md),
[A2A Protocol Plugin Development Plan](DEVELOPMENT_PLAN.md)

## Goal

Let A2A tasks handled by `a2a_protocol_plugin` be served by
[omp / oh-my-pi](https://github.com/can1357/oh-my-pi). omp is a fork of pi
that adds hash-anchored edits, LSP/DAP, a persistent Python/JS `eval`, a
browser, subagents (`task`), MCP and a native tool-approval model. It is
bridged the same way as goose, pi, Hermes and OpenClaw. The bridge is
`OmpAgentHandler` (`agent_type: "omp"`), deployed as an `omp` compose
profile in `docker-a2a-gateway`.

```
A2A client ──▶ silvaengine_gateway ──▶ silvaengine_daemon ──▶ a2a_protocol_plugin
                                                                 │ OmpAgentHandler (omp dialect of SidecarAgentHandler)
                                                                 │ (PiSidecarClient, HTTP + SSE — unchanged)
                                                                 ▼
                       omp sidecar (pi sidecar + omp dialect) ── stdin/stdout JSONL ──▶ omp --mode rpc
                       (container: omp)                             one process per active session
```

omp is also in the BanyanOS plan's coordinated-agent table as a future A2A
target. Once `agent_type: "omp"` is published, the orchestrator reaches it
through the same uniform A2A route as the other agents. No orchestration
changes are needed.

## How the existing plans shape this one

| Lesson | From | Applied here |
| --- | --- | --- |
| Run the agent in its own container behind a small HTTP + SSE service, with an `X-Secret-Key` shared secret. Keep its tools out of the gateway. | goose, pi | Same container model and auth. |
| Capture a real transcript in a spike before writing the handler, and replay it in unit tests. | goose G1, pi P1 | O1 spike → `tests/omp_rpc_transcript.json`. |
| Keep wire details in one transport file. | `goose_acp_client.py`, `pi_client.py` | The omp dialect lives in the **sidecar**. The plugin reuses `pi_client.py` — one `label` keyword was added, every pi message stays byte-identical. |
| Map conversation key → backend session per tenant. Never forward a client id. | goose, pi, core engine | `conversation_map.py`, `setting_id = "omp_session#{key}"`. |
| Use one shared streaming/reasoning contract. Never tag a copy of the answer as reasoning, and never turn replayed history into tokens. | pi "Streaming and reasoning" | Same contract and assertions (below). |
| Read config from agent metadata → setting → `os.environ` → `Config` → default. The secret prefers setting/env over metadata. | goose (`4eb3dbd`), pi | Same order, `omp_*` / `OMP_*` keys. |
| The host env can leak into the child agent (pi exports `PI_MODEL`/`PI_PROVIDER`). | pi P1 finding | Use the `OMP_SIDE_*` namespace, and strip `PI_*`/`OMP_*` model vars from the child env. |
| Reuse; don't build a duplicate adapter. | BanyanOS plan | Make the pi stack dialect-aware rather than forking it. |
| Advertise only capabilities you have verified per target. | BanyanOS plan | The Agent Card for omp states its cancel/approval limits after O1. |

## Implementation status

**Files that exist on `omp-integration`.**

| Area | Files |
| --- | --- |
| Plugin bridge | `handlers/sidecar_handler.py` (the shared `SidecarAgentHandler` base — session mapping, run lifecycle, cancel, approval flow, event → A2A chunk mapping, one table-driven config lookup), `handlers/pi_handler.py` (`PiAgentHandler`, the pi dialect), `handlers/omp_handler.py` (`OmpAgentHandler`, the omp dialect), `handlers/pi_client.py` (the transport; one `label` keyword added, the pi messages unchanged) |
| Plugin config / registration | `handlers/config.py` (`omp_*` attributes + the `OMP_*` block), `handlers/a2a_ai_agent_utility.py` (`AGENT_TYPE_MAP["omp"]`), `tests/register_omp_agent.py` |
| Plugin tests | `tests/test_omp_handler.py` (34 tests), `tests/test_omp_executor.py` (6 tests), `tests/test_omp_live.py` (gated on `A2A_RUN_LIVE_OMP_TESTS=1`), `tests/omp_rpc_transcript.json` (the trimmed spike frames) |
| Sidecar | `pi/src/dialect.ts` (`AgentDialect` + `createDialect()`), the dialect-driven `pi/src/rpc.ts` / `server.ts` / `pool.ts` / `index.ts`, `pi/Dockerfile` (`AGENT_DIALECT=pi|omp`; the omp variant installs the SHA-256-verified standalone omp binary), `pi/test/dialect.test.ts`, `pi/test/fake-omp.mjs` |
| Deployment | compose service `omp` (profile `omp`, port 3310), `.env.example` "Option G", `scripts/generate_env.py`, `scripts/seed_agents.py` (seeds `a2a-omp-agent`), the `Makefile` targets `omp-up` / `omp-down` / `omp-test` / `omp-chat`, `tests/omp/*` |

**What the spike proved** — the frame shapes and the runtime facts listed in
[Verified by the O1 spike](#verified-by-the-o1-spike-omp-18182): the `ready`
handshake and the v2 negotiation, `prompt_result` as the completion signal
(`completed` / `aborted` / `error`), `agentInvoked:false` for a prompt that
started no run, the native approval `select` with its exact option labels, the
`tool_execution_*` shapes, the flat session-file layout, and the absence of any
`PI_*` / `OMP_*` variable in omp's own environment.

**What the live run proved** (2026-11-10, against a real omp 18.18.2
container): the sidecar's HTTP + SSE surface behaves as designed — session
create, `prompt` → `run_end` `end_turn`, a provider failure →
`run_end` `stopReason:"error"`, `abort` → `run_end` `aborted:true`, the
native approval `select` (`Approve` / `Approve <tool> for this session` /
`Deny`) answered through `/ui-response`, and the busy path (409 without
`streamingBehavior`, 200 + `{"disposition":"followUp"}` with it). The plugin's
gated live suite (`tests/test_omp_live.py`) is green against that sidecar
(5 passed, 1 skipped — the skipped one needs the durable PostgreSQL
mapping), and an A2A `message/send` through the real executor returns the
model's answer plus `metadata.omp` (session id, session file, stop reason).
The run also exposed one defect, now fixed and covered by a regression test:
the omp run window used to close at the prompt's yield, so a follow-up
queued during the run answered into a closed window (the window now stays
open until `get_state.isSettled`).

**What is left** — the gateway-level end to end: `docker compose --profile omp
up -d omp` together with the `a2a-gateway` container, then `tests/omp/*`
(`message/stream`, `tasks/get`, `tasks/cancel` through the gateway). On the
machine this ran on, the local compose stack could not be recreated while
`compose.projects.local.yaml` includes a missing
`compose.postgres-recovery.local.yaml`, so the gateway was left running
untouched and the gateway-level criteria below stay unchecked.

## Interface options

omp's integration surfaces (from omp `docs/cli-reference.md`, `docs/rpc.md`;
checked against v18.18.2, released 2026-11-08):

| Interface | What it is | Decision |
| --- | --- | --- |
| **`omp --mode rpc`** | JSONL over stdio, a superset of pi's RPC, with a `ready` handshake and protocol v1/v2 negotiation. Official clients exist for TS, Python, Rust and Go. | **Used**, behind the sidecar. |
| `omp --mode rpc-ui` | RPC plus tool-UI frames. | Not used: O1 showed that plain `--mode rpc` already surfaces the tool-approval dialog (an `extension_ui_request` `select`). |
| `omp acp` | ACP over **stdio** (for Zed and similar editors). | Not used now. Our ACP client (`goose_acp_client.py`) speaks ACP over **HTTP**, so this would need a new stdio↔HTTP bridge, and we would lose omp-only commands (subagents, `get_messages_page`, `goal`). Kept as the fallback if the RPC dialect drifts. |
| `-p` / `--mode json` | One-shot runs. | Not used: no cancel and no follow-ups. |
| HTTP server | None. (`omp auth-gateway serve` is a credential proxy, not an agent API.) | — |

Architecture options:

| Option | Verdict |
| --- | --- |
| **A. Reuse the pi sidecar with a `dialect: "omp"` adapter.** Same image layout, binary `omp`. The sidecar normalises omp events to the `run_end` contract `pi_client.py` already consumes. | **Chosen.** About 80% of the code is shared. A protocol change touches one adapter file, and the plugin client and tests are reused. |
| B. A separate omp sidecar, forked from pi | Rejected: two copies of the pool, auth, timeouts and framing fixes. |
| C. ACP-stdio sidecar + `GooseAcpClient`-style handler | Fallback only (see above). |
| D. Spawn omp inside the gateway | Rejected for the same reasons as pi option B (runtime in the gateway image, tools in the gateway, breaks serverless). |

## omp facts the design relies on

Items marked **(verify)** were not confirmed from the docs; they are settled by
the O1 spike, which ran on 2026-11-10 — see
[Verified by the O1 spike](#verified-by-the-o1-spike-omp-18182) below.

- **Install / runtime.** npm `@oh-my-pi/pi-coding-agent`, binary `omp`.
  The npm package requires **Bun ≥ 1.4.4** (its bin is a `.ts` file). Prebuilt
  standalone binaries (`omp-linux-x64`, `-arm64`, `-musl-*`; ~210–270 MB)
  need no Bun. **The image uses the pinned standalone binary**, and the
  sidecar stays on Node 24. Pin `OMP_VERSION=18.18.2` (verify SHA-256 like
  the official installer).
- **Startup.** omp writes
  `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1,2],"maxFrameBytes":1048576,...}`
  before it reads commands. The sidecar must wait for `ready` (pi had no
  such frame). v1 caps stdout frames at 1 MiB. v2 (`negotiate_protocol`)
  splits large frames into base64 `rpc_chunk` frames. **Negotiate v2**, so
  large tool results or `get_messages` cannot fail.
- **Framing.** JSONL, not JSON-RPC 2.0. Commands carry an optional `id`,
  echoed in `{type:"response", command, success, data|error}`. Malformed
  input gives a `parse` failure. Closing stdin drains, disposes and exits 0.
- **Completion signal: the key dialect difference.** omp has **no
  `agent_settled`**. Each prompt ends with
  `prompt_result {id, agentInvoked, status: completed|aborted|error, sessionSettled, error?}`,
  and quiescence is `session_settled` (or `get_state.isSettled`). A
  built-in slash command returns `agentInvoked:false` and no
  `prompt_result` (the analogue of pi's `disposition: "handled"`). A
  provider failure is `success:true` on the prompt response, with
  `agent_end.stopReason:"error"` / `prompt_result.status:"error"`.
- **Streaming events.** Same as pi: `message_start/_update/_end` (plus
  `messageId`), with `assistantMessageEvent` `text_delta`, `thinking_delta`
  and `toolcall_*`. Also `tool_execution_start/_update/_end`, plus new
  `tool_stream_update`, `subagent_*`, `notice`, `goal_updated`,
  `model_changed`, `auto_compaction_*`, `rpc_frame_error`. Exact JSON shape
  of `tool_execution_*` **(verify)**. `set_event_filter
  {messageUpdates:"delta"}` can trim the stream.
- **Commands used.** `prompt` (`streamingBehavior` steer|followUp), `abort`,
  `new_session`, `switch_session {sessionPath}`, `open_session {sessionDir}`,
  `get_state`, `get_session_stats`, `set_model`, `set_thinking_level`
  (`inherit, off, minimal, low, medium, high, xhigh, max`). omp does not
  serve `get_commands`; use `get_available_commands`.
- **Sessions.** Root `~/.omp` (`PI_CONFIG_DIR` renames the directory). Files
  go in `~/.omp/agent/sessions/<encoded-cwd>/<ts>_<uuidv7>.jsonl`, entry
  version 3. `--session-dir` / `PI_CODING_AGENT_SESSION_DIR` override this.
  Sessions use an ownership lease: a second writer **forks into a sibling
  file**. The process pool must therefore never run two processes on one
  session, and the sidecar index must follow a fork if one happens
  **(verify)**.
- **Config / providers.** `~/.omp/agent/config.yml`, `models.yml` (custom
  OpenAI-compatible: `providers.<name>.{baseUrl, api: openai-completions, apiKey|auth: none, models}`),
  `.env` files. `OMP_*` mirror `PI_*`. Standard provider vars
  (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `OPENROUTER_API_KEY`, …,
  `OPENAI_BASE_URL`). Whether omp exports `PI_MODEL`/`OMP_MODEL` like pi
  does **(verify)**.
- **Approvals: native, unlike pi.** Each tool has a tier (read/write/exec).
  `tools.approvalMode` = `always-ask` | `always-ask-cached` | `write` |
  `yolo` (**the default**). Per-tool overrides: `tools.approval.<tool>:
  allow|deny|prompt`. `tools.restrictedFolders` and `bash.patterns` apply
  too. `--approval-mode` sets the mode, and `--yolo` forces yolo. In RPC, a
  prompt arrives as an `extension_ui_request` **`select`** with the choices
  and details **(verify exact options)**. An unanswerable prompt fails
  closed. Subagents run headless as yolo, and any `prompt` policy rejects
  them.
- **Extension UI.** Same as pi (`select/confirm/input/editor`, plus new `ask`
  (multi-question, after `set_ask_dialog`), `cancel {targetId}`, `open_url`).
  The response is `value | confirmed | cancelled (+timedOut) | answers[]`.
  Secret `input` (OAuth login) is rejected in RPC.

### Verified by the O1 spike (omp 18.18.2)

The spike ran `omp --mode rpc` (the pinned standalone release binary, SHA-256
checked against the release `SHA256SUMS.txt`) inside a `node:24-bookworm-slim`
container on 2026-11-10. The provider was a local OpenAI-compatible mock
(`models.yml` provider `compat`) — the only reachable API key had no credits.
Transcript: `a2a_protocol_plugin/tests/omp_rpc_transcript.json` (trimmed) and
`docker-a2a-gateway/docs/test_results/omp_review/spike_transcript.json` (raw).

- **`ready` frame** — exactly as documented:
  `{"type":"ready","protocolVersion":1,"supportedProtocolVersions":[1,2],"maxFrameBytes":1048576,"maxReassembledFrameBytes":67108864,"supportsBashExcludeFromContext":true}`.
  `negotiate_protocol {protocolVersion:2}` → `success:true`,
  `data.protocolVersion:2`.
- **Prompt completion** — the prompt response of a normal turn carries **no**
  `data`; the run ends with
  `prompt_result {id, agentInvoked:true, status:"completed", sessionSettled:true}`.
  A **local-only builtin slash command** (`/usage`, `/model`, …) answers the
  prompt response with `data.agentInvoked:false` and **no** `prompt_result`
  follows; it also emits `command_output {text}` frames.
- **Provider failure** — `prompt_result {status:"error", error:{message,
  provider, model, retryable, httpStatus}}`. A 5xx is retried first
  (`auto_retry_start`, up to 10 attempts) before the failure surfaces; a 4xx
  fails immediately.
- **Abort** — `prompt_result {status:"aborted", agentInvoked:true}`.
- **Busy** — a prompt while the agent streams and **without**
  `streamingBehavior` fails: response `success:false`, error
  `"Agent is already processing. Use steer() or followUp() to queue messages,
  or wait for completion."`, plus a `prompt_result` with
  `agentInvoked:false, status:"error"`. With `streamingBehavior` the message
  is queued and shows up in `queue_update {steering, followUp, liveSteered}`.
- **`tool_execution_*`** — `start {toolCallId, toolName, args}`,
  `update {toolCallId, toolName, args, partialResult}`,
  `end {toolCallId, toolName, result:{content:[{type:"text",text}],details}, isError}`.
- **Approval** — omp's native approval arrives as
  `extension_ui_request {id, method:"select", title:"Allow tool: bash\nCommand: …",
  options:["Approve","Approve bash for this session","Deny"]}`. The details
  ride the **`title`** (there is no `message` field) and the frame carries
  **no** `sessionId`. Plain `--mode rpc` emits it (no `--mode rpc-ui` needed).
- **`get_state`** — `sessionId`, `sessionFile`, `isStreaming`, `isSettled`,
  `queuedMessageCount`, `queuedMessages{steering,followUp,liveSteered}`,
  `hasPendingAsyncWork`, `messageCount`, `thinkingLevel`, `model`. There is
  **no** `pendingMessageCount` (that is pi's field name).
- **Session files** — with `--session-dir <dir>` the files are flat:
  `<dir>/<timestamp>_<sessionId>.jsonl` (no `<encoded-cwd>` sub-directory).
- **Lease / fork** — a second process **may** open the same session file
  (`switch_session` → `success:true, cancelled:false`); the file did **not**
  fork and `get_state.sessionFile` stayed the same. The lease is advisory
  (`get_session_lease` probes ownership), so the sidecar still compares
  `sessionFile` after a switch and follows a fork if one ever happens.
- **Env leakage** — omp injects **no** `PI_*` / `OMP_*` variable into its own
  environment; the sidecar strips its own `PI_*`/`OMP_*` model variables from
  the spawned child.
- **Custom endpoint** — `~/.omp/agent/models.yml` with
  `providers.<name>.{baseUrl, api: openai-completions, apiKey, models:[{id,
  reasoning, input}]}`; `reasoning: true` enables the thinking levels.
- **Memory** — one idle omp process: `VmRSS ≈ 261 MB` (peak ≈ 301 MB), so
  `OMP_MAX_PROCESSES` (8) needs ≈ 2 GB.

## Streaming and reasoning: follow the existing bridges

omp adds **no new streaming path**. It inherits the shared contract in
[PI_INTEGRATION_PLAN.md § Streaming and reasoning](PI_INTEGRATION_PLAN.md#streaming-and-reasoning-follow-the-existing-bridges)
unchanged: `text_delta` → `token`; `thinking_delta` → `token` +
`meta.data_format = "reasoning"`; one SDK Message after the stream; and
`show_reasoning` applied by the bridge. Ignore the full-content copies in
`thinking_end` / `message_end`. Forward only the current run's events.
omp-specific additions:

| omp event | Bridge chunk |
| --- | --- |
| `tool_stream_update` | ignored (logged). Optionally coalesce into `tool_result` later. |
| `subagent_lifecycle/progress/event` | ignored. `set_subagent_subscription {level:"off"}` at spawn. Subagent text must **never** become answer tokens. |
| `prompt_result.status = error` | `error` → FAILED with partial content. |
| `notice`, `goal_updated`, `ttsr_triggered`, `todo_reminder` | ignored (logged). |

Add one row to the "How each bridge sources reasoning" table: **omp** —
`message_update → assistantMessageEvent.thinking_delta` → tagged `token`.

## Design

### 1. Sidecar: make `docker-a2a-gateway/pi/` dialect-aware

- `src/dialect.ts` (new): an interface with `pi` and `omp` implementations.
  It covers the handshake (omp: await `ready`, then `negotiate_protocol` v2),
  frame reassembly (`rpc_chunk`), the run-completion predicate (pi:
  `agent_settled` + state poll; omp: `prompt_result` for the prompt `id`,
  then `session_settled`), the "handled" case (pi `disposition: handled` /
  omp `agentInvoked:false`), and the spawn flags.
- `server.ts` / `pool.ts` call the dialect instead of hard-coding
  `agent_settled`. **The HTTP + SSE API and the `run_end` frame do not
  change**, so `pi_client.py` works for both.
- Selection: `AGENT_DIALECT=omp`, `AGENT_BIN=omp` (keep `PI_BIN` as an alias).
  Env namespace `OMP_SIDE_*` (mirrors `PI_SIDE_*`). Strip host
  `PI_*`/`OMP_*` model vars from the child env, as `pool.spawnEnv` does now.
- omp spawn flags: `--mode rpc --session-dir /data/sessions`,
  `--approval-mode <OMP_SIDE_APPROVAL_MODE>` (default `always-ask`),
  `--no-lsp` unless `OMP_SIDE_NO_LSP=0`, `--max-cost <usd>` from
  `OMP_SIDE_MAX_COST`, and the per-session `--model <spec>` / `--thinking
  <level>` from the dialect's `spawnFlags()` (the whole model spec in one
  flag; `inherit` is not a CLI value). After `ready`: `negotiate_protocol
  {protocolVersion: 2}` and `set_subagent_subscription {level: "off"}`.
- **Approvals.** omp gates tools natively, so the `tool-approval.js`
  extension is **not mounted** for omp. The sidecar forwards omp's approval
  `select` to the bridge as today. The bridge's `omp_permission_mode` logic
  answers it: the allow / deny option is matched by label
  (`omp_approval_allow_labels` / `omp_approval_deny_labels`) or by the
  conventional words, and a dialog whose options cannot be classified is
  **cancelled** (fail closed — the bridge never approves blindly).
- Port `OMP_PORT` 3310; volumes `omp-data:/data`,
  `omp-agent:/root/.omp/agent`, `./www/omp-workspace:/workspace`.

### 2. Plugin handler: `handlers/omp_handler.py` (`OmpAgentHandler`)

- The shared bridge logic lives in `handlers/sidecar_handler.py`
  (`SidecarAgentHandler`): the 9 repeated `metadata/setting/env/Config`
  lookups are one table-driven loop over class attributes
  (`CONFIG_PREFIX`, `ENV_PREFIX`, `SESSION_MAP_PREFIX`, `RESULT_KEY`,
  `AGENT_LABEL`, `DIALOG_METHODS`, `DEFAULT_API_URL`). pi's behaviour is
  unchanged, and `test_pi_handler.py` stays green.
- `handlers/pi_handler.py` keeps the public `PiAgentHandler` name as a thin
  **pi dialect** subclass; `handlers/omp_handler.py` adds `OmpAgentHandler`,
  the **omp dialect** subclass (siblings, not one derived from the other).
  The omp dialect declares `omp` / `OMP` / `omp_session`, the default sidecar
  address `http://localhost:3310`, and overrides only:
  - permission answering for an approval `select` (choose the allow option /
    deny option by label instead of "first option"; an unclassifiable dialog
    is cancelled — fail closed);
  - `thinking` level validation (omp's own set, `inherit` … `max`; an
    unknown level is dropped, so the sidecar default applies);
  - the approval label lists `omp_approval_allow_labels` /
    `omp_approval_deny_labels`;
  - an error message that names `OMP_SIDECAR_SECRET_KEY` / `OMP_SECRET_KEY`.
- Configuration:

  | Metadata key | Setting / env | Default |
  | --- | --- | --- |
  | `omp_api_url` | `OMP_API_URL` | `http://localhost:3310` |
  | `omp_secret_key` (fallback only) | `OMP_SECRET_KEY` (= sidecar `OMP_SIDECAR_SECRET_KEY`) | — |
  | `omp_cwd` | `OMP_CWD` | `/workspace` |
  | `omp_model` | `OMP_MODEL` (`provider/modelId` or role `smol`/`slow`) | sidecar default |
  | `omp_thinking` | `OMP_THINKING` | sidecar default |
  | `omp_permission_mode` | `OMP_PERMISSION_MODE` | `auto_allow` |
  | `omp_busy_behavior` | `OMP_BUSY_BEHAVIOR` | `followUp` |
  | `omp_timeout` | `OMP_STREAM_TIMEOUT` | `300` |
  | `omp_approval_timeout` | `OMP_APPROVAL_TIMEOUT` | `120` |
  | `omp_approval_allow_labels` | `OMP_APPROVAL_ALLOW_LABELS` | `""` (fall back to the conventional allow words) |
  | `omp_approval_deny_labels` | `OMP_APPROVAL_DENY_LABELS` | `""` (fall back to the conventional deny words) |

- Run id `"{sessionId}:{runSeq}"`. `cancel_run()` → `POST …/abort` →
  `prompt_result.status: aborted` → CANCELED.
- Cost guard (optional, after O1): forward `--max-cost` per agent through
  `OMP_SIDE_MAX_COST`, because omp tools (eval, browser, subagents) can run
  long.

### 3. Registration

`AGENT_TYPE_MAP["omp"]` in `a2a_ai_agent_utility.py`; `omp_*` attributes +
`_set_parameters` in `handlers/config.py`; `tests/register_omp_agent.py`.
No new entity, no migration, and no RLS change (the session map goes in
`a2a_settings`).

## Phases

Status: O1–O5 are implemented (see [Implementation status](#implementation-status));
what is still open is the live end-to-end run and the live suites.

### O1: Spike

- Throwaway container with the pinned `omp-linux-x64` binary. Drive
  `omp --mode rpc` and capture `tests/omp_rpc_transcript.json`: `ready` +
  v2 negotiation, plain answer, thinking, a tool call (`bash`/`write`),
  `abort` mid-run, a prompt while streaming (with and without
  `streamingBehavior`), `switch_session` resume (no replay as deltas), a
  slash command (`agentInvoked:false`), a provider error, and
  `--approval-mode always-ask` with a `bash` call (the exact approval
  `select` frame).
- Settle every **(verify)**: `tool_execution_*` shapes, approval options
  and their labels, env var leakage (`PI_MODEL`/`OMP_MODEL`), lease-fork
  behaviour on resume after a sidecar restart, `models.yml` reuse of
  `OPENAI_COMPAT_*`, memory per process (to size `OMP_MAX_PROCESSES`), and
  whether `--no-lsp`/`--no-pty` are needed headless.

### O2: Sidecar dialect

- `src/dialect.ts` + refactor of `server.ts`/`pool.ts`. The pi vitest suite
  must stay green. Add omp vitest cases for `ready`, `rpc_chunk`
  reassembly, `prompt_result` completion, and `agentInvoked:false`.
- `omp/Dockerfile` (or a build arg on `pi/Dockerfile`), with the binary
  pinned and its checksum verified, plus a healthcheck and entrypoint that
  seed `config.yml`/`models.yml` from `.env`.

### O3: Plugin handler

- Extract the shared base `handlers/sidecar_handler.py`
  (`SidecarAgentHandler`), keep `PiAgentHandler` as the pi dialect, add the
  omp dialect (`handlers/omp_handler.py`), config wiring, and
  `AGENT_TYPE_MAP["omp"]`.

### O4: Tests

- `tests/test_omp_handler.py` (sidecar mocked from the O1 transcript) — 34
  tests, green:
  - [x] first turn / resumed turn through `omp_session#` mapping; never a
        client-supplied id; a cross-tenant key starts a new session
  - [x] answer vs tagged reasoning; `show_reasoning` off drops it (same
        assertions as `TestStreamingShowReasoning`)
  - [x] subagent events and `tool_stream_update` never reach the answer
  - [x] tool call / result chunks
  - [x] abort → CANCELED; `prompt_result.status: error` → FAILED
  - [x] busy session → followUp / steer / reject
  - [x] approval `select` per permission mode, and the `ask` timeout
  - [x] metadata cannot override the secret; env fallback
  - [x] 401 / 404 / 409 / connection-drop
- [x] `test_pi_handler.py` is unchanged and green after the refactor (52
  tests).
- [x] An executor-level test via `A2ADaemonEngine.a2a()` (like
  `test_goose_executor.py`): send, echoed `contextId` resume, tenant
  isolation — `tests/test_omp_executor.py` (6 tests, green).
- [x] `tests/test_omp_live.py` gated on `A2A_RUN_LIVE_OMP_TESTS=1` (+
  `OMP_LIVE_URL` / `OMP_LIVE_KEY`). It uses the repo's env-var `skipif`
  convention, so no new pytest marker is needed. **The live run happened**
  (2026-11-10, against a live omp sidecar): 5 passed, 1 skipped (the skip
  needs the durable PostgreSQL mapping, like the pi live suite).

### O5: Deployment (`docker-a2a-gateway`, `omp` profile)

- `omp` compose service, seeded `a2a-omp-agent`, an `.env.example` section,
  and `generate_env.py` (`OMP_SIDECAR_SECRET_KEY` = `OMP_SECRET_KEY`, port
  offset, `OPENAI_COMPAT_*` → `models.yml`).
- `tests/omp/*`: the five-script set copied from `tests/pi/*`
  (`StreamSections`, `--hide-reasoning`), plus `make omp-up` / `omp-down`
  / `omp-test` / `omp-chat`.

### O6: Docs

- `AGENTS.md` (layout, live-test gate, transcript re-capture note),
  `DEVELOPMENT_PLAN.md` Phase 5, gateway README (bridge, sibling and
  reasoning tables, security notes), and the BanyanOS coverage table row.

## Acceptance Criteria

- [ ] `message/send`, `message/stream`, `tasks/get` and `tasks/cancel` work
      end to end against a live omp sidecar (pinned version). — **partly
      done**: `message/send` works live (the real executor against a live
      omp 18.18.2 sidecar returns the model's answer and `metadata.omp`), and
      the sidecar's abort path was exercised live; the **gateway**-level
      round trips (`message/stream`, `tasks/get`, `tasks/cancel`) are still
      open — see "What is left".
- [x] An echoed `contextId` / `metadata.thread_uuid` resumes the same omp
      session. Another tenant's key never does. (`test_omp_handler.py`,
      `test_omp_executor.py`)
- [x] The streaming contract holds: answer and reasoning stay separate
      artifacts, `metadata.reasoning` is populated, and `show_reasoning=false`
      drops the reasoning. (`test_omp_handler.py`)
- [ ] The same scripts as pi and goose check it live (tagged reasoning
      sections, answer-only HTTP fallback). — **open: needs the gateway-level
      run** (the `tests/omp/*` scripts are in place and the protocol-level
      script ran against the live sidecar).
- [x] Subagent output never leaks into the answer. (`test_omp_handler.py`)
- [x] `tasks/cancel` → CANCELED: the handler's abort path is covered
      (`abort` → CANCELED, `cancel_run()` aborts the sidecar session). The
      live `tasks/cancel` round trip is part of the end-to-end run above.
- [x] Tool approvals follow `omp_permission_mode` through omp's native
      approval mode: `auto_allow` picks the allow option, `deny` the deny
      option, `ask` parks the task `INPUT_REQUIRED` and resumes on the
      approval response, and an unclassifiable dialog or the `ask` timeout
      fails closed. (`test_omp_handler.py`; the live approval round trip was
      exercised against the live sidecar — the `select` dialog with the
      `Approve` / `Approve <tool> for this session` / `Deny` options and the
      answer through `/ui-response`.)
- [ ] The pi bridge is unchanged: pi unit (52 tests) and vitest (36 tests,
      including the new `dialect.test.ts`) suites are green in this branch,
      but the pi **live** suite has not been re-run here.
- [x] Unit tests run without omp (`test_omp_handler.py`,
      `test_omp_executor.py` mock the sidecar).

## Risks

| Risk | Mitigation |
| --- | --- |
| omp releases very often (3 releases on 2026-11-07/08), and the RPC may drift. | Pin the binary and verify its checksum. Replay the O1 transcript. Keep the dialect in one sidecar file. Re-run live suites on every upgrade. ACP-stdio stays as a fallback. |
| The default `yolo` runs every tool, including `eval` (Python/JS), browser and `bash`, without approval. | Own container, workspace volume only, required secret. Set `--approval-mode` explicitly per permission mode. Restrict `--tools`. Keep `computer` off (it is the default). Consider a network policy. |
| Large binary (~250 MB) and heavier processes (LSP, Python kernel, browser). | Size `OMP_MAX_PROCESSES` from O1 measurements. Use `--no-lsp` by default. Use idle reaping. |
| Session leases fork on concurrent writers. | One process per session (pool invariant). Detect a fork on resume and update the sidecar index. |
| Subagents run headless as yolo and bypass `prompt` policies. | Disable `task` with `--tools` unless the agent opts in. Document this. |
| Extracting the shared base (`SidecarAgentHandler`) breaks pi. | Behaviour-preserving extraction with `test_pi_handler.py` (52 tests, unchanged) and the pi vitest suites as the gate; the omp dialect is added next to it, not on top of it. |
| Bun-only npm package. | Use the standalone binary. No Bun in the image. |

## References

- omp README: https://github.com/can1357/oh-my-pi
- omp RPC: https://github.com/can1357/oh-my-pi/blob/main/docs/rpc.md
- omp CLI reference: https://github.com/can1357/oh-my-pi/blob/main/docs/cli-reference.md
- omp approval mode: https://github.com/can1357/oh-my-pi/blob/main/docs/approval-mode.md
- omp sessions: https://github.com/can1357/oh-my-pi/blob/main/docs/session.md
- omp models / providers: https://github.com/can1357/oh-my-pi/blob/main/docs/models.md
- omp environment variables: https://github.com/can1357/oh-my-pi/blob/main/docs/environment-variables.md
- omp releases: https://github.com/can1357/oh-my-pi/releases
