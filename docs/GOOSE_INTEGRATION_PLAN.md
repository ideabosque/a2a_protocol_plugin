# Goose Integration Development Plan

Status: implemented (G1-G4 done; G5 in docker-a2a-gateway)
Last updated: 2026-10-15
Related: [A2A Protocol Plugin Development Plan](DEVELOPMENT_PLAN.md)

## Goal

Add a `GooseAgentHandler` bridge so A2A tasks handled by `a2a_protocol_plugin`
can be served by a [goose](https://github.com/aaif-goose/goose) agent server, in
the same way `HermesAgentHandler` and `OpenClawAgentHandler` front their
backends. The target deployment is a `docker-a2a-goose-gateway` stack, modeled
on `docker-a2a-openclaw-gateway`: SilvaEngine gateway + this plugin, plus a
sibling goose container.

```
A2A client ──▶ silvaengine_gateway ──▶ silvaengine_daemon ──▶ a2a_protocol_plugin
                                                                 │ GooseAgentHandler
                                                                 ▼
                                                     goose serve  (POST/GET /acp)
```

## Target Interface: `goose serve` (ACP over HTTP)

- Target **`goose serve`**, which exposes goose as an Agent Client Protocol
  (ACP) server at `/acp` over HTTP (SSE) and WebSocket. The default bind is
  `127.0.0.1:3284`.
- Do **not** target the legacy `goosed` REST/SSE API. Upstream issue
  aaif-goose/goose#6642 ("goosed to ACP-over-HTTP") was closed on 2026-07-09
  with ACP as the way forward, and its phase 4 removes `goosed`.
- **Auth:** the server requires `GOOSE_SERVER__SECRET_KEY` unless it is started
  with `--dangerously-unauthenticated`. Clients send the secret in the
  `X-Secret-Key` header on every `/acp` request.
- **Wire protocol** (ACP Streamable HTTP RFD, still a draft; see
  agentclientprotocol PR #721):
  - `POST /acp` `initialize` returns its result inline (200) plus an
    `Acp-Connection-Id` header.
  - Every later POST returns `202 Accepted` with an empty body. Results arrive
    on SSE streams and are matched by JSON-RPC `id`.
  - There are two `GET /acp` streams, both sent with
    `Accept: text/event-stream`:
    - The connection-scoped stream carries the `session/new` and
      `session/load` results.
    - The session-scoped stream (adds `Acp-Session-Id`) carries
      `session/update` notifications, the `session/prompt` result, and
      server→client requests such as `session/request_permission`.
  - The client answers a server request by POSTing a JSON-RPC response that
    carries both headers.
  - `session/cancel` is a session-scoped notification.
  - `DELETE /acp` closes the connection and its sessions.
  - The RFD says HTTP/2 is required. Verify this against the pinned goose
    build; if it holds, use `httpx[http2]`.

## Fit With the Existing Handler Contract

The handler is loaded dynamically from agent metadata (`module_name` /
`class_name`) by `load_agent_handler()` in `a2a_ai_agent_utility.py`, which
passes `logger=, agent_config=, setting=, context=`. It must implement:

```python
def ask_model(self, input_messages, context, stream_queue=None, stream_event=None) -> dict: ...
def cancel_run(self, run_id: str) -> bool: ...
def resolve_approval(self, run_id: str, approved: bool, reason: str = "") -> bool: ...
```

Streaming chunks go to `stream_queue` as `{"name", "value", "meta"}`. Names
used today: `run_id`, `token`, `tool_call`, `tool_result`, `approval`,
`error`.

## Design

1. **Isolate the transport.** Put the ACP-over-HTTP mechanics in
   `handlers/goose_acp_client.py` (`GooseAcpClient`):
   - initialize
   - open and parse the SSE streams
   - match responses to requests by id
   - send JSON-RPC responses to server requests
   - send notifications
   - close the connection

   Because the transport is a draft, header names, `Acp-Protocol-Version`,
   SSE framing, and a later switch to WebSocket should change only this file.
   `goose_handler.py` speaks ACP methods only. Use synchronous `httpx`, like
   the other handlers, with an `http_transport` injection point for tests.
2. **One ACP connection per `ask_model` call.** The handler is created for each
   request, and gateway/Lambda processes are not sticky:
   1. `initialize`, checking `agentCapabilities.loadSession` and
      `promptCapabilities`.
   2. Open the connection stream.
   3. `session/new` or `session/load`.
   4. Open the session stream.
   5. `session/prompt`.
   6. Drain updates until the prompt result arrives.
   7. `DELETE /acp` in a `finally` block.

   goose persists sessions itself, so no process affinity is needed between
   turns.
3. **Map A2A `context_id` to a goose `sessionId`, scoped by tenant.**
   - On the first turn, `session/new` (`cwd`, `mcpServers`) creates the
     session. The handler returns `metadata.thread_uuid = sessionId`, so the
     executor adopts it as the `context_id`. This is the same path the core
     engine already uses (`effective_context_id` in `a2a_executor.py`).
   - Persist the mapping in `a2a_settings` as
     `setting_id = "goose_session#{context_id}"` under the request's
     `partition_key`. Use the repository layer (as
     `DurablePushNotificationConfigStore` does), so both backends work with no
     migration.
   - **Never pass a client-supplied id straight to `session/load`.** goose is
     not tenant-aware, so anyone who knows a session id could load another
     tenant's conversation. Resolve the id only through the partition-scoped
     mapping.
   - An unknown or missing mapping starts a new session. If `session/load`
     fails (404), start a new session and log a warning.
4. **Send only the new user turn.** goose keeps the conversation history, so
   unlike Hermes and OpenClaw do not resend `input_messages` history.
   - Convert A2A parts to ACP content blocks:
     - text → `text`
     - `FilePart` with bytes → `image` or `resource` (if `promptCapabilities`
       allows it)
     - `FilePart` with a URI → `resource_link`
     - `DataPart` → a JSON `text` block
   - Reject or downgrade a block type that the capabilities do not advertise.
5. **Map ACP updates to stream chunks:**

   | ACP update (`session/update` → `sessionUpdate`) | Bridge chunk |
   | --- | --- |
   | `agent_message_chunk` | `token` (answer text; also collected into `content`) |
   | `agent_thought_chunk` | `token` with `meta.data_format = "reasoning"` (kept out of the answer, as in the Hermes fix `753ce97`) |
   | `tool_call` | `tool_call` (title, kind, `toolCallId`, raw input) |
   | `tool_call_update` (`completed` / `failed`) | `tool_result` (content or error) |
   | `plan` | ignored, or a status `meta` only |
   | `user_message_chunk` / anything replayed during `session/load` | **dropped**: history replay must never become answer tokens |
   | `_goose/unstable/session/update` `usage_update` / `message_usage` | stored in result `metadata.usage` (for cost tracking) |
   | `session/prompt` result `stopReason` | `end_turn` → completed; `cancelled` → canceled; `max_tokens` / `max_turn_requests` / `refusal` → partial `content` + `error` |

   - Only accept `session/update` notifications that arrive after the
     `session/prompt` POST. This drops the replay from `session/load`.
6. **Permissions** (`session/request_permission`) follow a
   `goose_permission_mode` policy:
   - `auto_allow`: answer with the `allow_once` option. This is the default
     for trusted, sandboxed goose containers.
   - `deny`: answer with `reject_once`.
   - `ask`: emit an `approval` chunk, which makes the executor emit
     `INPUT_REQUIRED`, then block until `resolve_approval(run_id, approved)`
     posts the chosen `optionId` (or `{"outcome": "cancelled"}`) or
     `goose_approval_timeout` expires (then reject).

   `ask` works only when the same process holds the session stream (daemon
   mode). The executor's `_active_external_runs` registry is in-process, so
   serverless deployments must use `auto_allow`/`deny`, or set goose's own
   `GOOSE_MODE` on the server. Document this; do not silently fall back.
7. **Cancel.** `run_id` is `"{connectionId}:{sessionId}"`, pushed to the bridge
   as a `run_id` chunk.
   - `cancel_run()` sends `session/cancel` on that connection and sets
     `stream_event`.
   - The pending prompt then resolves with `stopReason: cancelled`, and the
     task ends `CANCELED`.
8. **Non-streaming.** `goose serve` has no single-shot endpoint. Run the
   streaming flow with an internal queue and return the aggregated
   `{"content", "role", "metadata"}`.
9. **Configuration.** Resolve each value in the order agent metadata →
   `setting` → `Config` → default, as in Hermes:

   | Metadata key | Setting / env | Default |
   | --- | --- | --- |
   | `goose_api_url` | `GOOSE_API_URL` | `http://localhost:3284` |
   | `goose_secret_key` | `GOOSE_SECRET_KEY` (prefer env/setting over metadata) | — |
   | `goose_cwd` | `GOOSE_CWD` | `/workspace` |
   | `goose_mcp_servers` | `GOOSE_MCP_SERVERS` (JSON) | `[]` |
   | `goose_permission_mode` | `GOOSE_PERMISSION_MODE` | `auto_allow` |
   | `goose_timeout` | `GOOSE_STREAM_TIMEOUT` | `300` |
   | `goose_approval_timeout` | `GOOSE_APPROVAL_TIMEOUT` | `120` |

   Provider and model selection stay on the goose server (`GOOSE_PROVIDER`,
   `GOOSE_MODEL`, provider credentials, `GOOSE_DISABLE_KEYRING=1`). Never log
   or persist the secret key.
10. **Errors.**
    - `401` / `403` → task FAILED with an auth message.
    - `404` on the connection → re-initialize once.
    - `406` / `415` → a transport bug (raise).
    - The draft defines no SSE resume (`Last-Event-ID`), so a dropped stream
      fails the task with whatever partial content was collected.

## Phases

### Phase G1: Transport Spike

- Run a pinned `goose serve` locally and capture a real ACP exchange
  (`initialize` → `session/new` → `session/prompt` with a tool call →
  `session/request_permission` → `session/cancel` → `session/load` →
  `DELETE`).
- Confirm the details the draft leaves open:
  - whether HTTP/2 is required
  - SSE event framing
  - the exact update method names
  - the `request_permission` option ids
  - the `session/load` replay shape
- Save the captured transcript as a test fixture
  (`tests/goose_acp_transcript.json`).

### Phase G2: ACP Client

- Implement `handlers/goose_acp_client.py`:
  - connection lifecycle
  - SSE parsing
  - routing responses by request id
  - server-request callbacks
  - notifications
  - close
- Unit-test it with `httpx.MockTransport` against the captured fixture.

### Phase G3: Handler

- Implement `handlers/goose_handler.py`:
  - `ask_model` (streaming and non-streaming)
  - `cancel_run`
  - `resolve_approval`
  - content-block conversion
  - mapping updates to stream chunks
  - permission policy
- Add `goose_*` class attributes and `_set_parameters` wiring to
  `handlers/config.py` (mirror `hermes_*` / `openclaw_*`).
- Add the partition-scoped `context_id` → `sessionId` mapping helpers on
  `a2a_settings`.

### Phase G4: Tests

- `tests/test_goose_handler.py` (mocked transport):
  - [ ] first turn and resumed turn
  - [ ] replay dropped on `session/load`
  - [ ] reasoning tagging
  - [ ] tool calls
  - [ ] each `stopReason`
  - [ ] cancel
  - [ ] each permission mode
  - [ ] the `ask` timeout
  - [ ] a cross-tenant `context_id` that is never loaded
  - [ ] the 401 / 404 paths
- `tests/test_goose_live.py`, gated on `A2A_RUN_LIVE_API_TESTS=1` and
  `GOOSE_API_URL`.
- `tests/register_goose_agent.py`, which registers an agent with metadata
  `module_name = "a2a_protocol_plugin.handlers.goose_handler"` and
  `class_name = "GooseAgentHandler"`.
- Executor-level test: `message/send` + `tasks/get` + `tasks/cancel` through
  `A2ADaemonEngine.a2a()` with the goose handler mocked at the transport.

### Phase G5: Deployment (`docker-a2a-goose-gateway`, separate repo)

- A gateway image with this plugin installed, plus a `goose` service running
  `goose serve --host 0.0.0.0 --port 3284 --with-builtin developer`.
- A shared `GOOSE_SERVER__SECRET_KEY`, provider credentials as env vars, and
  `GOOSE_DISABLE_KEYRING=1`.
- A dedicated workspace volume mounted at `goose_cwd`, with no host mounts.
- `routes.yaml`, a README, and hello/SSE/chatbot live scripts (copy the
  OpenClaw set). Record the pinned goose version.

### Phase G6: Docs

- Add `goose` to the `agent_type` list in `DEVELOPMENT_PLAN.md` Phase 5 and to
  `AGENTS.md`.
- Document the permission modes and the serverless limitation of `ask`.

## Acceptance Criteria

- The A2A methods `message/send`, `message/stream`, `tasks/get` and
  `tasks/cancel` work end to end against a live `goose serve`.
- A second turn with the returned `contextId` resumes the same goose session.
- Streaming emits answer tokens without duplicated or replayed text. Reasoning
  is tagged and tool calls are surfaced.
- A permission request is handled per policy. `ask` produces `INPUT_REQUIRED`
  and resumes on approval in daemon mode.
- One tenant can never load another tenant's goose session.
- Unit tests run without a goose server. The live tests pass against the
  pinned goose version recorded in the docker stack.

## Risks

| Risk | Mitigation |
| --- | --- |
| goose's ACP-over-HTTP transport is a draft RFD and may change (headers, SSE framing, HTTP/2, WebSocket). | Keep all wire details in `goose_acp_client.py`, pin the goose version in the docker stack, and run the live tests on every goose upgrade. |
| goose sessions are not tenant-aware. | Resolve `context_id` → `sessionId` only through the partition-scoped `a2a_settings` mapping; never forward client-supplied session ids. |
| `session/load` replays history as `session/update` events. | Drop updates that arrive before the `session/prompt` POST; add a regression test (same class of bug as the Hermes duplicate-reply fix). |
| Interactive tool approval needs the session stream held open in one process. | Default to `auto_allow` in a sandboxed goose container; allow `ask` only in daemon mode and document it. |
| goose runs tools (shell, file edits) with the agent's privileges. | Run goose in its own container with a scoped workspace volume, no host mounts, and network policy; the secret key is required. |

## Implementation Notes (2026-10-15, goose 1.53.0)

Phase G1 (transport spike) ran against a live `goose serve` 1.53.0; the
captured exchange is stored as `tests/goose_acp_transcript.json`. Wire facts
that differ from (or pin down) the draft RFD:

- `--dangerously-unauthenticated` is NOT honored in 1.53.0; the server always
  requires `GOOSE_SERVER__SECRET_KEY`. Clients send it as `X-Secret-Key`
  (401 when missing/wrong).
- The client must send an `Acp-Connection-Id` (any UUID) with the
  `initialize` POST; the server returns its own canonical id in the
  `acp-connection-id` response header and every later request MUST use that
  id (a foreign id is answered with HTTP 404).
- `initialize` returns 200 inline (application/json); every later POST
  returns 202 with an empty body. HTTP/1.1 is sufficient (no HTTP/2 needed).
- Routing: the connection-scoped SSE stream carries the `session/new`
  result; the session-scoped stream (opened with `Acp-Session-Id`) carries
  `session/load` results, `session/update` notifications, prompt results,
  and server->client requests. Each message is delivered on exactly one
  stream.
- `session/set_mode` is a regular request (result `{}`) and emits a
  `current_mode_update`; modes observed: `auto`, `approve`, `smart_approve`,
  `chat`.
- `session/prompt` result carries `stopReason`
  (`end_turn`/`cancelled`/`max_tokens`/...) and `usage`
  (input/output/total tokens).
- `session/request_permission` arrives as a server request with
  `params.options` = `allow_always`/`allow_once`/`reject_once`/
  `reject_always`; the client answers by POSTing
  `"outcome": {"outcome": "selected", "optionId": ...}` with both
  headers (a `cancelled` outcome is also accepted).
- If the client advertises `fs` capabilities (read/writeTextFile), goose
  delegates file operations to the CLIENT via `fs/write_text_file` /
  `fs/read_text_file` server requests — an unanswered request hangs the run.
  The handler therefore advertises `fs/terminal` as `false` and lets goose's
  own tools do the work.
- A second `session/prompt` while a run is active fails with JSON-RPC
  -32602 ("session already has active run ...; use
  _goose/unstable/session/steer"). The handler cancels + retries once.
- `session/load` replays the whole conversation as `session/update`
  notifications (user/agent/thought chunks) — confirmed. The client uses
  epoch-marked updates (not wall-clock) to filter replay; see
  `GooseAcpClient.prompt`.
- `session/cancel` (notification) resolves the pending prompt promptly
  (~1.2 s observed mid-run). Cancelling before the run starts is a no-op,
  the same class of edge as OpenClaw (no server-side stop).

Delivered in G1-G4: `handlers/goose_acp_client.py`,
`handlers/goose_handler.py`, config wiring (`goose_*`),
`AGENT_TYPE_MAP["goose"]`, `tests/goose_acp_transcript.json`,
`tests/test_goose_handler.py` (31 mocked-transport tests),
`tests/test_goose_live.py` (gated on `A2A_RUN_LIVE_GOOSE_TESTS=1`) and
`tests/register_goose_agent.py`.

## References

## References

- goose CLI commands (`goose serve`): https://goose-docs.ai/docs/guides/goose-cli-commands/
- Running a remote goose server: https://goose-docs.ai/docs/guides/remote-goose-server/
- goose ACP reference: https://goose-docs.ai/docs/gdk/acp/reference/
- aaif-goose/goose#6642 (goosed → ACP over HTTP): https://github.com/aaif-goose/goose/issues/6642
- ACP Streamable HTTP & WebSocket transport RFD: https://agentclientprotocol.com/rfds/streamable-http-websocket-transport
- ACP transports: https://agentclientprotocol.com/protocol/v2/transports
