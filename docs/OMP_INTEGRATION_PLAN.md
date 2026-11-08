# omp (oh-my-pi) Integration Development Plan

Status: PROPOSED — documentation only; nothing implemented. The O1 spike has not run.
Last updated: 2026-11-08
Branch (proposed): `omp-integration` (a2a_protocol_plugin + docker-a2a-gateway)
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
                                                                 │ OmpAgentHandler (subclass of PiAgentHandler)
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
| Keep wire details in one transport file. | `goose_acp_client.py`, `pi_client.py` | The omp dialect lives in the **sidecar**. The plugin reuses `pi_client.py` unchanged. |
| Map conversation key → backend session per tenant. Never forward a client id. | goose, pi, core engine | `conversation_map.py`, `setting_id = "omp_session#{key}"`. |
| Use one shared streaming/reasoning contract. Never tag a copy of the answer as reasoning, and never turn replayed history into tokens. | pi "Streaming and reasoning" | Same contract and assertions (below). |
| Read config from agent metadata → setting → `os.environ` → `Config` → default. The secret prefers setting/env over metadata. | goose (`4eb3dbd`), pi | Same order, `omp_*` / `OMP_*` keys. |
| The host env can leak into the child agent (pi exports `PI_MODEL`/`PI_PROVIDER`). | pi P1 finding | Use the `OMP_SIDE_*` namespace, and strip `PI_*`/`OMP_*` model vars from the child env. |
| Reuse; don't build a duplicate adapter. | BanyanOS plan | Make the pi stack dialect-aware rather than forking it. |
| Advertise only capabilities you have verified per target. | BanyanOS plan | The Agent Card for omp states its cancel/approval limits after O1. |

## Interface options

omp's integration surfaces (from omp `docs/cli-reference.md`, `docs/rpc.md`;
checked against v18.18.2, released 2026-11-08):

| Interface | What it is | Decision |
| --- | --- | --- |
| **`omp --mode rpc`** | JSONL over stdio, a superset of pi's RPC, with a `ready` handshake and protocol v1/v2 negotiation. Official clients exist for TS, Python, Rust and Go. | **Used**, behind the sidecar. |
| `omp --mode rpc-ui` | RPC plus tool-UI frames. | Evaluate in O1 only if `rpc` does not surface tool-approval dialogs. |
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

Items marked **(verify)** are not confirmed from docs and are settled in O1.

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
- omp spawn flags: `--mode rpc --session-dir /data/sessions --no-title`
  (or `PI_NO_TITLE=1`), `--approval-mode <OMP_SIDE_APPROVAL_MODE>`
  (default `always-ask` when the bridge permission mode is `ask`/`deny`,
  else `yolo`), `--no-lsp` unless enabled, `--tools` from
  `OMP_SIDE_TOOLS`. After `ready`: `set_subagent_subscription off` and
  `set_event_filter {messageUpdates:"delta"}`.
- **Approvals.** omp gates tools natively, so the `tool-approval.js`
  extension is **not mounted** for omp. The sidecar forwards omp's approval
  `select` to the bridge as today. The bridge's `pi_permission_mode` logic
  answers it, and the "allow" option is mapped from the select options
  (O1 decides how).
- Port `OMP_PORT` 3310; volumes `omp-data:/data`,
  `omp-agent:/root/.omp/agent`, `./www/omp-workspace:/workspace`.

### 2. Plugin handler: `handlers/omp_handler.py` (`OmpAgentHandler`)

- Refactor `PiAgentHandler` so its config keys come from class attributes
  (`CONFIG_PREFIX = "pi"`, `ENV_PREFIX = "PI"`, `SESSION_MAP_PREFIX =
  "pi_session"`). There are 9 repeated `metadata/setting/env/Config`
  lookups today (`pi_handler.py:120-186`). Behaviour must not change, and
  the existing `test_pi_handler.py` must stay green.
- `OmpAgentHandler(PiAgentHandler)` sets `omp` / `OMP` / `omp_session`. It
  overrides only the following:
  - permission answering for an approval `select` (choose the allow option /
    deny option by label instead of "first option");
  - `thinking` level validation (adds `inherit`, `minimal`, `xhigh`);
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

- Prefix refactor of `PiAgentHandler`, then `omp_handler.py`, config
  wiring, and `AGENT_TYPE_MAP["omp"]`.

### O4: Tests

- `tests/test_omp_handler.py` (sidecar mocked from the O1 transcript):
  - [ ] first turn / resumed turn through `omp_session#` mapping; never a
        client-supplied id; a cross-tenant key starts a new session
  - [ ] answer vs tagged reasoning; `show_reasoning` off drops it (same
        assertions as `TestStreamingShowReasoning`)
  - [ ] subagent events and `tool_stream_update` never reach the answer
  - [ ] tool call / result chunks
  - [ ] abort → CANCELED; `prompt_result.status: error` → FAILED
  - [ ] busy session → followUp / steer / reject
  - [ ] approval `select` per permission mode, and the `ask` timeout
  - [ ] metadata cannot override the secret; env fallback
  - [ ] 401 / 404 / 409 / connection-drop
- `test_pi_handler.py` is unchanged and green after the refactor.
- An executor-level test via `A2ADaemonEngine.a2a()` (like
  `test_goose_executor.py`): send, echoed `contextId` resume, tenant
  isolation.
- `tests/test_omp_live.py` gated on `A2A_RUN_LIVE_OMP_TESTS=1` (+
  `OMP_LIVE_URL` / `OMP_LIVE_KEY`). Register the marker in `pytest.ini`.

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
      end to end against a live omp sidecar (pinned version).
- [ ] An echoed `contextId` / `metadata.thread_uuid` resumes the same omp
      session. Another tenant's key never does.
- [ ] Streaming meets the shared contract (separate answer and reasoning
      artifacts, `metadata.reasoning`, answer-only HTTP fallback,
      `show_reasoning=false`), checked with the same scripts as pi and goose.
- [ ] Subagent output never leaks into the answer.
- [ ] `tasks/cancel` → CANCELED.
- [ ] Tool approvals follow `omp_permission_mode` through omp's native
      approval mode. `ask` → `INPUT_REQUIRED` and resumes in daemon mode.
- [ ] The pi bridge is unchanged: pi unit, vitest and live suites still pass.
- [ ] Unit tests run without omp.

## Risks

| Risk | Mitigation |
| --- | --- |
| omp releases very often (3 releases on 2026-11-07/08), and the RPC may drift. | Pin the binary and verify its checksum. Replay the O1 transcript. Keep the dialect in one sidecar file. Re-run live suites on every upgrade. ACP-stdio stays as a fallback. |
| The default `yolo` runs every tool, including `eval` (Python/JS), browser and `bash`, without approval. | Own container, workspace volume only, required secret. Set `--approval-mode` explicitly per permission mode. Restrict `--tools`. Keep `computer` off (it is the default). Consider a network policy. |
| Large binary (~250 MB) and heavier processes (LSP, Python kernel, browser). | Size `OMP_MAX_PROCESSES` from O1 measurements. Use `--no-lsp` by default. Use idle reaping. |
| Session leases fork on concurrent writers. | One process per session (pool invariant). Detect a fork on resume and update the sidecar index. |
| Subagents run headless as yolo and bypass `prompt` policies. | Disable `task` with `--tools` unless the agent opts in. Document this. |
| Refactoring `PiAgentHandler` breaks pi. | Behaviour-preserving refactor first, in its own commit, with `test_pi_handler.py` and the pi live suite as the gate. |
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
