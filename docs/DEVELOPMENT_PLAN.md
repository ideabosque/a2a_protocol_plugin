# A2A Protocol Plugin Development Plan

Status: planning baseline
Last updated: 2026-09-10

## Goal

`a2a_protocol_plugin` owns the A2A protocol module that is currently embedded in
`a2a_daemon_engine`. It should depend on `silvaengine_daemon` for shared gateway, partition, config,
repository, GraphQL, and SSE primitives, while keeping all A2A-specific SDK and
wire-protocol behavior here.

## Source Analysis

Source project: `C:\Users\bibo7\gitrepo\silvaengine\a2a_daemon_engine`

Current implementation includes:

- Gateway dispatch functions in `a2a_daemon_engine.main`: `dispatch_a2a`,
  `dispatch_agent_card`, `dispatch_graphql`, and `dispatch_sse_message`.
- A2A SDK integration through `handlers/a2a_server.py`,
  `handlers/a2a_executor.py`, `handlers/a2a_jsonrpc_bridge.py`, and
  `handlers/a2a_taskstore.py`.
- Protocol features for `message/send`, `message/stream`, `tasks/get`,
  `tasks/list`, `tasks/cancel`, `tasks/resubscribe`, push notification config,
  and extended agent card methods.
- A2A metadata entities: agents, tasks, messages, and settings, with DynamoDB
  and PostgreSQL implementations plus Alembic migrations.
- Backend bridge handlers: `a2a_proxy_handler`, `hermes_handler`,
  `core_engine_handler`, and `openclaw_handler`.
- Test coverage for SDK v1.0 behavior, JSON-RPC bridge, executor behavior,
  push config, PostgreSQL backend, and backend bridge handlers.

## Target Ownership

Keep in this plugin:

| Area | Target modules |
| --- | --- |
| A2A gateway surface | `main.py`, `deploy()`, `dispatch_a2a`, `dispatch_agent_card`, A2A GraphQL action manifest. |
| A2A protocol runtime | SDK server builder, JSON-RPC bridge, task store, executor, state mapping, push notification config, extended cards. |
| A2A entities | Agent, task, message, setting GraphQL types/queries/mutations and entity repositories. |
| A2A backend bridges | `a2a_proxy`, Hermes, Core Engine, OpenClaw, and the common handler resolution map. |
| A2A tests/docs | Existing protocol compliance, bridge, live, and PostgreSQL tests adapted to the new package name. |

Move to or consume from `silvaengine_daemon`:

- Partition defaulting and context normalization.
- Base config, logger/settings access, RLS session lifecycle.
- Per-plugin module YAML descriptor consumed by `silvaengine_daemon` internal
  plugin routing.
  `silvaengine_gateway` dispatches external requests to `silvaengine_daemon`,
  and `silvaengine_daemon` dispatches matching A2A routes to this plugin.
- Repository base/registry interfaces.
- Generic GraphQL execution helpers.
- Generic SSE manager primitives.
- JSON/datetime/model serialization helpers.

## Proposed Package Shape

```text
a2a_protocol_plugin/
|-- a2a_protocol_plugin/
|   |-- __init__.py
|   |-- main.py
|   |-- schema.py
|   |-- handlers/
|   |   |-- config.py
|   |   |-- a2a_server.py
|   |   |-- a2a_executor.py
|   |   |-- a2a_jsonrpc_bridge.py
|   |   |-- a2a_taskstore.py
|   |   |-- a2a_sse.py
|   |   |-- a2a_pushconfig.py
|   |   |-- a2a_pushconfig_store.py
|   |   |-- a2a_extended_card.py
|   |   |-- a2a_ai_agent_utility.py
|   |   |-- a2a_proxy_handler.py
|   |   |-- hermes_handler.py
|   |   |-- core_engine_handler.py
|   |   `-- openclaw_handler.py
|   |-- models/
|   |-- queries/
|   |-- mutations/
|   |-- types/
|   `-- tests/
|-- migration/
|-- docs/
|   `-- DEVELOPMENT_PLAN.md
|-- pyproject.toml
`-- README.md
```

## Migration Phases

### Phase 1: Bootstrap Package

- Create `pyproject.toml` with `a2a-sdk[http-server]`, `graphene`, `httpx`,
  `pydantic`, `python-jose`, `SilvaEngine-Utility`, and optional PostgreSQL,
  telemetry, and gRPC extras.
- Copy A2A-only source files from `a2a_daemon_engine` into
  `a2a_protocol_plugin` with import paths renamed.
- Keep public dispatch names route-compatible unless gateway manifests are
  updated in the same deployment.
- Add the A2A plugin module YAML descriptor required by
  `silvaengine_daemon` internal plugin routing.

### Phase 2: Replace Duplicated Core Helpers

- Import partitioning, serialization, async runner, config base, GraphQL wrapper,
  repository contracts, and generic SSE primitives from `silvaengine_daemon`.
- Keep `A2AProtocolConfig` as a plugin config subclass for A2A settings such as
  default agent, stream timeout, input/output modes, and backend handler map.
- Remove duplicated `utils/normalization.py`, `utils/rls.py`, and local
  `_apply_partition_defaults` after equivalent tests pass.

### Phase 3: Stabilize A2A Entities and Persistence

- Move A2A DynamoDB/PostgreSQL models and repositories into the plugin.
- Keep Alembic migrations for `a2a_agents`, `a2a_tasks`, `a2a_messages`, and
  `a2a_settings` with the existing `context_id` and RLS behavior.
- Verify dual-backend tests against the new import paths.

### Phase 4: Preserve Protocol Compliance

- Port protocol tests: JSON-RPC bridge, executor, task store, push config,
  extended card, streaming, and agent card URL rewriting.
- Run the A2A TCK/checker scripts against standalone and gateway-dispatched
  modes.
- Document the known gateway streaming binding if live SSE continues to flow
  through the separate gateway SSE route.

### Phase 5: Bridge Handler Cutover

- Keep `agent_type` selection for `a2a_proxy`, `hermes`, `core_engine`, and
  `openclaw`.
- Validate non-streaming, streaming, cancel, and approval/input continuation for
  each handler using mocked transports.
- Run live optional tests where services are available.

## Acceptance Criteria

- `a2a_protocol_plugin` installs and imports without `mcp` or Banyanos packages.
- `a2a_protocol_plugin` declares and imports its shared runtime dependency as
  `silvaengine_daemon`.
- Gateway routes for A2A JSON-RPC, agent card, A2A GraphQL, and SSE messages are
  registered from this plugin through `silvaengine_daemon` internal plugin
  routing YAML and the A2A plugin module YAML.
- Outside traffic flows through `silvaengine_gateway` first, then
  `silvaengine_daemon`, then this plugin.
- Existing A2A tests are ported and passing under the new package name.
- Agent cards, task states, push notifications, context IDs, and multimodal parts
  match the old package behavior or have an explicit compatibility note.
- `a2a_daemon_engine` can be retired or reduced to a temporary compatibility
  wrapper.

## Risks

| Risk | Mitigation |
| --- | --- |
| A2A SDK types leak into `silvaengine_daemon`. | Keep all SDK imports inside this plugin and test dependency boundaries. |
| Gateway streaming behavior diverges from strict A2A clients. | Document the binding and add compliance tests for the advertised interface. |
| Bridge handlers carry internal backend coupling. | Keep them behind the existing `ask_model`/cancel/approval handler contract. |
