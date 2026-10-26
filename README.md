# a2a_protocol_plugin

Implements SilvaEngine's Agent-to-Agent (A2A) protocol endpoints using the official
A2A SDK. Shared gateway dispatch, partitioning, persistence primitives, GraphQL,
serialization, async helpers, and SSE infrastructure come from `silvaengine_daemon`.
Authentication is handled by the gateway.

Agent handlers connect A2A tasks to core SilvaEngine agents, Hermes, OpenClaw,
goose, and pi. The pi handler communicates with the HTTP/SSE sidecar in
`docker-a2a-gateway/pi`, which runs pi in RPC mode and retains session files for
conversation resume. Queued steering and follow-up requests leave the original
caller's run open; only its owning request closes the live session process.

See [development guidance](AGENTS.md), the [development plan](docs/DEVELOPMENT_PLAN.md),
and the [pi integration plan](docs/PI_INTEGRATION_PLAN.md).
