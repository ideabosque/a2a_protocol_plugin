# AGENTS.md

Guidance for coding agents working in `a2a_protocol_plugin`.

## What this project is

The A2A (Agent-to-Agent) protocol plugin for SilvaEngine daemon deployments. It replaces the
older `a2a_daemon_engine` package. Protocol semantics come from the official `a2a-sdk`
(`DefaultRequestHandler` + a custom `AgentExecutor` and `TaskStore`); shared plumbing comes from
the sibling package `silvaengine_daemon` (gateway dispatch, partitioning, RLS, repository
registry, SSE, serialization, async helpers). **Authentication is done by the gateway, not here.**

Do not re-implement anything that `silvaengine_daemon` already provides — import it. See
[docs/DEVELOPMENT_PLAN.md](docs/DEVELOPMENT_PLAN.md) for the ownership split.

## Layout

```
a2a_protocol_plugin/
  main.py                 Entry points: deploy(), A2ADaemonEngine, dispatch_* functions
  schema.py               Graphene Query/Mutations for the admin CRUD API
  handlers/
    config.py             Config class (class-level state, backend selection, DB session)
    a2a_server.py         Wires SDK: agent card, task store, executor, push-config store
    a2a_executor.py       A2ADaemonExecutor — routes operations, emits SDK events
    a2a_ai_agent_utility.py  Bridge to agent handlers (load, build input, stream, persist)
    a2a_taskstore.py      SDK TaskStore backed by the repository layer
    a2a_pushconfig*.py    Durable push configs (stored in a2a_settings) + SSRF URL validator
    a2a_jsonrpc_bridge.py JSON-RPC dict <-> SDK protobuf request/response conversion
    a2a_core.py, a2a_utility.py, a2a_handlers.py  Persistence helpers / legacy operations
    core_engine_handler.py, hermes_handler.py, openclaw_handler.py,
goose_handler.py (+ goose_acp_client.py)  Agent handlers
  models/{dynamodb,postgresql}/      Backend models (pynamodb / SQLAlchemy)
  models/repositories/{dynamodb,postgresql}/  Repo classes registered with silvaengine_daemon
  queries/, mutations/, types/       GraphQL resolvers and types, one file per entity
  utils/rls.py            A2A table list for RLS policies
  tests/                  pytest suite (see below)
migration/                Alembic migrations (PostgreSQL only)
config/                   Example runtime config (*.example — never commit real values)
```

Entities: `a2a_agent`, `a2a_task`, `a2a_message`, `a2a_setting`. Each has a model per backend, a
repo per backend, a GraphQL type, a query module and a mutation module.

## Request flow

1. Gateway calls a module-level `dispatch_*` function in `main.py` (or the `A2ADaemonEngine`
   method of the same name via `deploy()` in serverless mode).
2. `apply_partition_defaults(params)` builds `partition_key = "{endpoint_id}#{part_id}"`.
3. `A2ADaemonEngine.a2a()` maps the JSON-RPC `method` to an SDK request type and calls the SDK
   request handler with a `ServerCallContext` whose `state` carries `partition_key`, agent/thread
   ids and request metadata.
4. The SDK drives `A2ADaemonExecutor.execute()`, which resolves the agent record, loads its
   handler class dynamically (`module_name` / `class_name` from the agent's DB metadata), calls
   `ask_model(...)`, and emits task/status/artifact events.
5. `DynamoDBA2ATaskStore` (works on both backends despite the name) persists tasks under the
   per-request partition from `context.state["partition_key"]`.

`message/stream` and `tasks/subscribe` collect all SDK events and return them in a single POST
response; live tokens reach clients through the gateway SSE manager (`silvaengine_daemon.sse`).

## Rules that matter

- **Multi-tenancy is mandatory.** Every read/write must be scoped by `partition_key`. Never let
  client-supplied data (e.g. `params.metadata`) override the gateway-derived `partition_key`;
  trusted values must be set *after* merging untrusted ones.
- **Both backends must keep working.** Any change to an entity touches the DynamoDB model + repo
  *and* the PostgreSQL model + repo, plus a new Alembic revision in `migration/alembic/versions/`
  (numbered `000N_description.py`). New PG tables need RLS policies (see `0005_enable_rls_policies.py`
  and `utils/rls.py`).
- **PostgreSQL session hygiene.** RLS context is set with a connection-level `SET`
  (`Config._set_rls_context`). Every entry point that sets it must call
  `Config.db_session.remove()` in a `finally` block, as the `dispatch_*` functions do.
- **Go through the repository layer.** Resolvers and handlers call
  `silvaengine_daemon.repositories.get_repo("<entity>")`; do not import backend models directly
  outside `models/` and `models/repositories/`.
- **Cache invalidation.** List resolvers use `@method_cache`; if you add an entity or a
  dependency, update `CACHE_ENTITY_CONFIG_*` and `CACHE_RELATIONSHIPS_*` in `handlers/config.py`.
- **Push webhooks** must pass `WebhookUrlValidator` (anti-SSRF allowlist). Don't bypass it.
- **JSON-RPC errors:** map typed SDK errors through `JSON_RPC_ERROR_CODE_MAP`; use `-32603` only
  for genuinely unexpected failures. Keep the legacy method-name aliases in `main.py` working.
- Return protocol responses via `silvaengine_daemon.serialization.dumps`, not `json.dumps`.

## Adding an agent handler

A handler is any class loaded by `load_agent_handler()` in `a2a_ai_agent_utility.py`. It is
constructed with `logger=, agent_config=, setting=, context=` and must implement:

```python
def ask_model(self, input_messages, context, stream_queue=None, stream_event=None) -> dict: ...
def cancel_run(self, run_id: str) -> bool: ...                      # cancellation
def resolve_approval(self, run_id, approved, reason="") -> bool: ...  # optional, approvals
```

When `stream_queue` is given, push chunks to it and set `stream_event` when done. Tag reasoning
chunks as reasoning (see `_is_reasoning_meta`) so they are not duplicated in the final reply.
Use `hermes_handler.py` / `openclaw_handler.py` / `goose_handler.py` as references;
register test agents with
`tests/register_*_agent.py`.

## Setup and tests

`silvaengine_daemon` lives in the sibling directory `../silvaengine_daemon`. If it is not installed,
put it on `PYTHONPATH`:

```bash
PYTHONPATH="../silvaengine_daemon;." python -m pytest a2a_protocol_plugin/tests -q -o addopts="" -o log_cli=false
```

(Use `:` instead of `;` as the path separator on Linux/macOS.)

- Copy `a2a_protocol_plugin/tests/.env.example` to `.env` for integration tests. Never commit `.env`.
- `test_postgresql_backend.py` needs a reachable PostgreSQL (`PG_*` or `DATABASE_URL`); it errors
  without one.
- Live tests (`test_*_live.py`, `test_a2a_proxy_e2e.py`) only run with `A2A_RUN_LIVE_API_TESTS=1`
  and real endpoints.
- Filter with markers (`-m unit`, `-m a2a`, ...) — markers are declared in `tests/pytest.ini`
  and `conftest.py`; `--strict-markers` is on, so register new ones.
- Protocol compliance: `test_a2a_protocol_compliance.py`, `a2a_tck_checker.py`,
  `validate_agent_card.py`.
- Known stale failures: `test_jwt_validation.py` expects JWT secret validation that `Config`
  no longer performs (auth moved to the gateway).

Run migrations against PostgreSQL from the `migration/` directory:

```bash
DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/db alembic upgrade head
```

Optional extras: `postgresql`, `grpc`, `telemetry` (see `pyproject.toml`).

## Code style

- Python 3.10+, type hints, `from __future__` imports as in neighbouring files.
- Match the surrounding module's style; keep `__author__` headers.
- When fixing a tracked defect, leave a short dated comment explaining *why*
  (existing pattern: `# DEF-003 fix (2026-09-03): ...`).
- Lint with `ruff` (dev dependency).

## Known state / caveats

- Not wired into the runtime (only tests import them, or nothing does): `a2a_cost_extension`,
  `a2a_graphql_subscriptions`, `a2a_health_monitor`, `a2a_secure_passport`, `a2a_telemetry`,
  `a2a_rate_limiter`, `a2a_cancellation`, `a2a_grpc`. Check before building on them.
- Names left over from `a2a_daemon_engine` remain (`A2ADaemonEngine`, the "A2A Daemon"
  service in `deploy()`, `tests/README.md`). Keep the public class/dispatch names stable — the
  gateway resolves them by string (e.g. `a2a_protocol_plugin.main:dispatch_agent_card`).
- Unused auth settings in `Config` (`jwt_*`, `admin_*`, `local_user_file`) are legacy.
