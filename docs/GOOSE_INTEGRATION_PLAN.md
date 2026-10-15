# Goose Integration Development Plan

Status: implemented and verified live against goose 1.53.0 (phases G1–G6);
no open items
Last updated: 2026-10-15
Related: [A2A Protocol Plugin Development Plan](DEVELOPMENT_PLAN.md)

## Goal

Let A2A tasks handled by `a2a_protocol_plugin` be served by a
[goose](https://github.com/aaif-goose/goose) agent, the same way
`HermesAgentHandler` and `OpenClawAgentHandler` front their backends. The
bridge is `GooseAgentHandler` (`agent_type: "goose"`). It is deployed by the
`goose` compose profile of `docker-a2a-gateway`, which runs goose as a sibling
container next to the SilvaEngine gateway, alongside the Hermes and OpenClaw
profiles.

```
A2A client ──▶ silvaengine_gateway ──▶ silvaengine_daemon ──▶ a2a_protocol_plugin
                                                                 │ GooseAgentHandler
                                                                 │ (GooseAcpClient)
                                                                 ▼
                                                 goose serve  POST/GET/DELETE /acp
```

## Interface: `goose serve` (ACP over HTTP)

goose is integrated through **ACP (Agent Client Protocol)**, served by
`goose serve` at `/acp`. There is no goose-specific "gateway" API; the
"gateway" is our SilvaEngine A2A gateway.

Why `goose serve` and not the alternatives:

| Option | Decision |
| --- | --- |
| **`goose serve`** (ACP over HTTP + SSE) | **Used.** goose runs in its own container, any gateway process can reach it, and goose persists sessions between requests, so serverless dispatch works. |
| `goose acp` (ACP over stdio) | Not used. It would put goose (and its tool execution) inside the gateway container and start a process per request. Same ACP methods, so a stdio transport could be added later as a second client class. |
| `goosed` (legacy REST + SSE) | Not used. Upstream issue aaif-goose/goose#6642 (closed 2026-07-09) made ACP the way forward and schedules `goosed` for removal. |

ACP over HTTP is still a draft RFD (agentclientprotocol PR #721). The behavior
below is what goose **1.53.0** actually does, captured in
`tests/goose_acp_transcript.json`; where it differs from the RFD, goose wins.

- **Auth.** `GOOSE_SERVER__SECRET_KEY` is always required —
  `--dangerously-unauthenticated` is not honored in 1.53.0. Clients send the
  secret as `X-Secret-Key`; a missing or wrong key returns 401.
- **Connection.** The client sends an `Acp-Connection-Id` (any UUID) with the
  `initialize` POST. The server replies 200 inline and returns its own
  canonical id in the `acp-connection-id` response header. Every later request
  must use that id; any other id returns 404.
- **POSTs.** Every POST after `initialize` returns `202` with an empty body.
  Results arrive on SSE streams, matched by JSON-RPC `id`. HTTP/1.1 is enough;
  HTTP/2 is not required.
- **Streams.** Two `GET /acp` streams (`Accept: text/event-stream`). Each
  message is delivered on exactly one of them:
  - connection-scoped: the `session/new` result;
  - session-scoped (adds `Acp-Session-Id`): `session/load` and
    `session/set_mode` results, `session/update` notifications, the
    `session/prompt` result, and server→client requests.
- **Modes.** `session/set_mode` is a normal request (result `{}`) and emits a
  `current_mode_update`. Modes: `auto`, `approve`, `smart_approve`, `chat`.
- **Prompt result.** `stopReason` (`end_turn`, `cancelled`, `max_tokens`, …)
  plus `usage` (input/output/total tokens).
- **Permissions.** `session/request_permission` arrives as a server request
  with `params.options` = `allow_always` / `allow_once` / `reject_once` /
  `reject_always`. The client POSTs a JSON-RPC response with both headers:
  `{"outcome": {"outcome": "selected", "optionId": "..."}}` (or
  `{"outcome": "cancelled"}`).
- **File system delegation.** If the client advertises `fs` capabilities,
  goose sends `fs/read_text_file` / `fs/write_text_file` requests to the
  client, and an unanswered request hangs the run. The client therefore
  advertises `fs` and `terminal` as `false`, so goose uses its own tools.
- **One run per session.** A second `session/prompt` during an active run
  fails with JSON-RPC `-32602` ("session already has active run …; use
  `_goose/unstable/session/steer`").
- **History replay.** `session/load` replays the whole conversation as
  `session/update` notifications (user, agent and thought chunks).
- **Cancel.** `session/cancel` (a notification) resolves the pending prompt
  in about 1.2 s mid-run. Cancelling before the run starts is a no-op, the
  same limitation OpenClaw has.
- **Close.** `DELETE /acp` closes the connection and its sessions.

## Handler Contract

`load_agent_handler()` in `a2a_ai_agent_utility.py` builds the handler from
agent metadata (`agent_type: "goose"` → `AGENT_TYPE_MAP["goose"]`) with
`logger=, agent_config=, setting=, context=`. It implements:

```python
def ask_model(self, input_messages, context, stream_queue=None, stream_event=None) -> dict: ...
def cancel_run(self, run_id: str) -> bool: ...
def resolve_approval(self, run_id: str, approved: bool, reason: str = "") -> bool: ...
```

Stream chunks go to `stream_queue` as `{"name", "value", "meta"}` with names
`run_id`, `token`, `tool_call`, `tool_result`, `approval`, `error`.

## Design (as built)

1. **Transport isolated in `handlers/goose_acp_client.py`** (`GooseAcpClient`):
   initialize, SSE stream readers, response routing by id, server-request
   callbacks, notifications, close. All wire details from the section above
   live here, so a change in the draft transport (headers,
   `Acp-Protocol-Version`, SSE framing, WebSocket) touches only this file.
   `goose_handler.py` works at the level of ACP methods. Synchronous `httpx`
   with an `http_transport` injection point for tests.
2. **One ACP connection per `ask_model` call.** The handler is created per
   request and gateway/Lambda processes are not sticky. Sequence:
   1. `initialize` (client capabilities: no `fs`, no `terminal`);
   2. open the connection stream;
   3. `session/new` or `session/load` (see 3);
   4. open the session stream;
   5. `session/set_mode` according to the permission policy (see 6);
   6. `session/prompt`, draining updates until the result arrives (on a
      `-32602` active-run error: cancel and retry once);
   7. `DELETE /acp` in a `finally` block.
3. **Conversation key → goose `sessionId`, scoped by tenant.**
   - The conversation key is the caller's explicit `thread_uuid`
     (`metadata.thread_uuid`, `metadata.contextId` or `params.threadId`),
     else the A2A `contextId` — the client's `message.contextId`, or the id
     the SDK generated on the first turn. The executor passes it to the
     bridge as `context["thread_uuid"]` / `context["context_id"]`, and the
     reply's `contextId` is always this key, so a client that simply echoes
     `contextId` continues the conversation.
   - First turn: `session/new` (`cwd`, `mcpServers`), and the mapping is
     stored in `a2a_settings` as `setting_id = "goose_session#{key}"` under
     the request's `partition_key`, through the shared
     `handlers/conversation_map.py` helpers (repository layer, both
     backends, no migration). Later turns `session/load` the mapped session.
   - The same helpers map the key to engine threads in the core-engine
     handler (`core_engine_thread#{key}`), and Hermes/OpenClaw history is
     stored and reloaded under the key, so all bridges behave the same.
   - A client-supplied id is **never** passed to `session/load`. goose is not
     tenant-aware, so the id is resolved only through the partition-scoped
     mapping. An unknown id, or a failed `session/load`, starts a new session.
4. **Only the new user turn is sent.** goose keeps the history, so unlike
   Hermes and OpenClaw the handler does not resend `input_messages` history.
   A2A parts become ACP content blocks: text → `text`; file bytes → `image`
   when `promptCapabilities` allows it; file URI → `resource_link`; data →
   JSON `text`.
5. **ACP updates → stream chunks:**

   | ACP update (`session/update` → `sessionUpdate`) | Bridge chunk |
   | --- | --- |
   | `agent_message_chunk` | `token` (answer text; also collected into `content`) |
   | `agent_thought_chunk` | `token` with `meta.data_format = "reasoning"`, kept out of the answer (same rule as the Hermes fix `753ce97`) |
   | `tool_call` | `tool_call` (title, kind, `toolCallId`, raw input) |
   | `tool_call_update` (`completed` / `failed`) | `tool_result` (content or error) |
   | `plan`, `current_mode_update` | ignored |
   | anything replayed by `session/load` | **dropped** — replay must never become answer tokens |
   | prompt result `usage` | `metadata.usage` |
   | prompt result `stopReason` | `end_turn` → completed; `cancelled` → canceled; `max_tokens` / `max_turn_requests` / `refusal` → partial `content` + `error` |

   Replay is filtered by **update epochs**, not timestamps: the stream reader
   stamps each `session/update` with the epoch current when it is read,
   `prompt()` bumps the epoch before its POST, and only updates stamped with
   that epoch are returned. SSE delivery is FIFO, so replayed updates always
   carry an older epoch.
6. **Permissions** (`goose_permission_mode`):
   - `auto_allow` (default, for sandboxed goose containers): session mode
     `auto`; any `session/request_permission` is answered `allow_once`.
   - `deny`: session mode `approve`, so goose asks for every tool call; each
     request is answered `reject_once`. (Until 2026-10-15 this mapped to
     `auto`, where goose never asks, so tools ran.)
   - `ask`: session mode `approve`; the handler emits an `approval` chunk (the
     executor replies `INPUT_REQUIRED`) and a worker thread waits for
     `resolve_approval(run_id, approved)` or `goose_approval_timeout`
     (then `reject_once`).

   `ask` only works when one process holds the session stream (daemon
   mode): the executor's `_active_external_runs` registry is in-process.
   Serverless deployments must use `auto_allow` or `deny`, or set goose's
   server-side `GOOSE_MODE`.
7. **Cancel.** `run_id = "{connectionId}:{sessionId}"`, sent as the first
   chunk. `cancel_run()` sends `session/cancel` on the active connection (or
   opens one if the handler has none), and rejects any pending `ask`
   approval so the run cannot hang. The prompt then resolves with
   `stopReason: cancelled` and the task ends `CANCELED`.
8. **Non-streaming** uses the same flow with an internal queue and returns the
   aggregated `{"content", "role", "metadata"}` (goose has no single-shot
   endpoint).
9. **Configuration.** Each value resolves in the order agent metadata →
   `setting` → `os.environ` → `Config` → default. The `os.environ` step covers
   gateway builds whose `settings.yaml` does not forward the `GOOSE_*` keys.
   The secret key is the exception: setting and env win over agent metadata,
   so tenant-writable metadata cannot override it (metadata is only a
   fallback).

   | Metadata key | Setting / env | Default |
   | --- | --- | --- |
   | `goose_api_url` | `GOOSE_API_URL` | `http://localhost:3284` |
   | `goose_secret_key` (fallback only) | `GOOSE_SECRET_KEY` (must equal the server's `GOOSE_SERVER__SECRET_KEY`) | — |
   | `goose_cwd` | `GOOSE_CWD` | `/workspace` |
   | `goose_mcp_servers` | `GOOSE_MCP_SERVERS` (JSON) | `[]` |
   | `goose_permission_mode` | `GOOSE_PERMISSION_MODE` | `auto_allow` |
   | `goose_timeout` | `GOOSE_STREAM_TIMEOUT` | `300` |
   | `goose_approval_timeout` | `GOOSE_APPROVAL_TIMEOUT` | `120` |

   Provider and model belong to the goose server (`GOOSE_PROVIDER`,
   `GOOSE_MODEL`, provider credentials, `GOOSE_MODE`,
   `GOOSE_DISABLE_KEYRING=1`). The secret key is never logged or persisted.
10. **Errors.** 401/403 → task FAILED with a message naming
    `GOOSE_SERVER__SECRET_KEY` / `GOOSE_SECRET_KEY`; 404 on session
    operations → new session; 406/415 → transport bug (raised). The draft has
    no SSE resume (`Last-Event-ID`), so a dropped stream fails the task with
    the partial content collected so far.

## Phases

### G1: Transport spike — done

Ran goose 1.53.0 and captured `initialize` → `session/new` → `set_mode` →
`session/prompt` with a tool call → `session/request_permission` →
`session/cancel` → `session/load` → `DELETE` into
`tests/goose_acp_transcript.json`. Findings are folded into
[Interface](#interface-goose-serve-acp-over-http).

### G2: ACP client — done

`handlers/goose_acp_client.py`, unit-tested with `httpx.MockTransport`
scripted from the captured transcript.

### G3: Handler — done

`handlers/goose_handler.py` (`ask_model`, `cancel_run`, `resolve_approval`,
content blocks, update mapping, permission policy, session mapping),
`goose_*` settings in `handlers/config.py`, and `AGENT_TYPE_MAP["goose"]` in
`a2a_ai_agent_utility.py`.

### G4: Tests — done

`tests/test_goose_handler.py` (mocked transport):

- [x] first turn and resumed turn (`session/load` via the mapping)
- [x] replay dropped on `session/load`
- [x] reasoning tagged and kept out of the answer
- [x] tool calls and results
- [x] each partial `stopReason`, and `end_turn`
- [x] cancel (prompt resolves as cancelled; bad run id)
- [x] each permission mode, and the `ask` timeout
- [x] the goose session mode sent for each permission policy
- [x] a client-supplied session id is never loaded
- [x] 401 and 404 paths; failed load falls back to a new session
- [x] `fs`/`terminal` never advertised; only the newest user turn is sent
- [x] env fallback; metadata cannot override the secret
- [x] active-run `-32602` retried after cancel


`tests/test_goose_executor.py` (real SDK `DefaultRequestHandler` +
`A2ADaemonExecutor` through `A2ADaemonEngine.a2a()`, goose mocked at the
transport):

- [x] `message/send` returns the goose reply and closes the ACP connection
- [x] echoing the returned `contextId` resumes the goose session
- [x] the same `metadata.thread_uuid` resumes the goose session, and the
      reply's `contextId` is that `thread_uuid`
- [x] the same `contextId` / `thread_uuid` under another tenant starts a new
      session

Also: `test_core_engine_handler.py::TestContextIdMapping` (contextId → engine
thread, never sent to the engine as a thread id) and
`test_phase10.py::TestConversationKey` (history and persistence keyed by
`thread_uuid` else `contextId`).
- `tasks/get` / `tasks/cancel` need the repository-backed task store and stay
  covered by the docker live suite.

Also delivered: `tests/test_goose_live.py` (gated on
`A2A_RUN_LIVE_GOOSE_TESTS=1`; the resume test skips when the `a2a_setting`
repository is unavailable, since the docker suite covers it against
PostgreSQL) and `tests/register_goose_agent.py`.

### G5: Deployment (`docker-a2a-gateway`, `goose` profile) — done

- `goose/` sibling image: pinned goose 1.53.0 release binary, supervisord,
  first-start provider config from `.env` (`GOOSE_MODEL_PROVIDER` =
  `openai_compat` / `anthropic` / `openrouter` / `ollama` / `gemini`), and a
  generated server secret when none is set.
- `goose serve --host 0.0.0.0 --port 3284 --with-builtin developer`; volumes
  for config, sessions, and a workspace mounted at `/workspace`.
- Seed agent `a2a-goose-agent` (`agent_type: "goose"`), `tests/goose/*`
  scripts, and `make goose-up` / `goose-down` / `goose-test` / `goose-chat`.
- Verified live: `test_gateway_live` 10/10, `test_sse_live` 7/7, chatbot
  streaming.

Fixed while testing the deployment:

- `silvaengine_gateway/settings.yaml` did not forward the `GOOSE_*` keys; the
  handler now also reads `os.environ` (`4eb3dbd`).
- `a2a_utility.insert_a2a_task` / `update_a2a_task` used GraphQL mutations
  that no longer matched the plugin schema (`partitionKey` argument,
  String/JSON types, missing `a2aTask` wrapper, nullable `$taskType`). This
  broke `tasks/cancel` persistence for **every** bridge (`43e1c29`,
  `a61c8cf`, `2817e89`).
- The gateway image cached the git-installed plugin layer; builds now take a
  `MODULE_STAMP` build arg (set it to the plugin commit) instead of needing
  `--no-cache`.

### G6: Docs — done

`DEVELOPMENT_PLAN.md` Phase 5 and `AGENTS.md` list the goose handler;
`docker-a2a-gateway` README and `.env.example` document the bridge and sibling
variables, permission modes, and the serverless limit of `ask`.

## Acceptance Criteria

- [x] `message/send`, `message/stream`, `tasks/get` and `tasks/cancel` work end
      to end against a live `goose serve`.
- [x] A second turn with the same `metadata.thread_uuid` resumes the same
      goose session.
- [x] A second turn that only echoes the returned `contextId` resumes the
      same goose session (verified live against goose 1.53.0).
- [x] Streaming emits answer tokens without duplicated or replayed text;
      reasoning is tagged and tool calls are surfaced.
- [x] A permission request is handled per policy (`auto_allow`, `deny`,
      `ask`).
- [x] One tenant can never load another tenant's goose session.
- [x] Unit tests run without a goose server; live tests pass against the
      pinned goose version (1.53.0).

## Open Items

None. Resolved on 2026-10-15:

- Echoing the returned `contextId` did not continue a conversation: the
  A2A `contextId` never reached the bridge, so goose opened a new session
  (and core engine a new thread, and Hermes/OpenClaw lost history) every
  turn, and the reply's `contextId` differed between the streaming and
  non-streaming paths. The bridge now keys every conversation by
  `thread_uuid` else `contextId`, returns that key as the reply's
  `contextId`, and the goose and core-engine handlers map it to their own
  ids per tenant (`handlers/conversation_map.py`).

- `deny` mapped to goose mode `auto`, where goose never asks, so tools ran.
  Now `approve` + `reject_once`, with a test for the mode sent per policy.
- Executor-level test added (`tests/test_goose_executor.py`).
- docker-a2a-gateway: the goose healthcheck (`goose/healthcheck.sh`) falls
  back to the generated secret and closes its ACP connection;
  `goose/Dockerfile` declares `ARG TARGETARCH`; `scripts/generate_env.py`
  generates `GOOSE_SERVER__SECRET_KEY` = `GOOSE_SECRET_KEY`, offsets
  `GOOSE_PORT`, and wires `GOOSE_MODEL_PROVIDER` / `GOOSE_MODEL` from
  `OPENAI_COMPAT_*`.

## Risks

| Risk | Mitigation |
| --- | --- |
| goose's ACP-over-HTTP transport is a draft RFD and may change (headers, SSE framing, WebSocket). | All wire details live in `goose_acp_client.py`; goose is pinned (1.53.0); re-capture the transcript and re-run the live tests on every goose upgrade. |
| goose sessions are not tenant-aware. | Resolve `thread_uuid` → `sessionId` only through the partition-scoped `a2a_settings` mapping; never forward client-supplied session ids. |
| `session/load` replays history as `session/update` events. | Epoch-based filtering in `GooseAcpClient.prompt`, with a regression test. |
| Interactive tool approval needs the session stream held open in one process. | Default to `auto_allow` in a sandboxed goose container; allow `ask` only in daemon mode. |
| goose runs tools (shell, file edits) with the container's privileges. | Own container, workspace volume only, required secret key; consider a network policy for the goose service. |
| A second prompt while a run is active is rejected by goose. | Cancel and retry once; long-term, consider `_goose/unstable/session/steer`. |

## References

- goose CLI commands (`goose serve`): https://goose-docs.ai/docs/guides/goose-cli-commands/
- Running a remote goose server: https://goose-docs.ai/docs/guides/remote-goose-server/
- goose ACP reference: https://goose-docs.ai/docs/gdk/acp/reference/
- aaif-goose/goose#6642 (goosed → ACP over HTTP): https://github.com/aaif-goose/goose/issues/6642
- ACP Streamable HTTP & WebSocket transport RFD: https://agentclientprotocol.com/rfds/streamable-http-websocket-transport
- ACP transports: https://agentclientprotocol.com/protocol/v2/transports
