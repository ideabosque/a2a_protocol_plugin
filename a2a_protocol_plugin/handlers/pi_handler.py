#!/usr/bin/python
"""
pi Agent Handler — A2A Bridge to the pi sidecar service

pi (https://github.com/earendil-works/pi, pi.dev) is a minimal,
self-extensible coding agent with a multi-provider LLM API. pi has NO
network server; the docker-a2a-gateway ``pi/`` compose sibling is a small
Node sidecar that spawns ``pi --mode rpc`` child processes over
stdin/stdout JSONL (the official pi RPC protocol, see pi docs rpc.md /
rpc-commands.md / json.md / rpc-extension-ui.md) and exposes HTTP + SSE.
This handler fronts the sidecar the same way ``GooseAgentHandler`` fronts
``goose serve``.

The bridge logic lives in ``handlers/sidecar_handler.py``
(``SidecarAgentHandler``) — the pi and the omp dialect share it. This class
only declares the pi vocabulary:

- metadata keys ``pi_*`` / setting and env keys ``PI_*``
- the session map ``pi_session#{key}`` in ``a2a_settings``
  (partition-scoped, see ``conversation_map.py``)
- the reply metadata key ``pi``
- the default sidecar address ``http://localhost:3300``
- the blocking extension-UI dialog methods

pi built-in tools run WITHOUT approval prompts (the pi process's own OS
permission is the boundary — we own the container; ``pi_permission_mode``
gates only extension UI dialogs, see docs/rpc-extension-ui.md).

Config (per-agent metadata -> setting -> env -> Config -> default):
- pi_api_url             / PI_API_URL             http://localhost:3300
- pi_secret_key          / PI_SECRET_KEY          (X-Secret-Key header;
    fallback only — env/setting win over tenant-writable agent metadata)
- pi_cwd                 / PI_CWD                 /workspace
- pi_model               / PI_MODEL               (provider/modelId; sidecar default)
- pi_thinking            / PI_THINKING            (off…max; sidecar default)
- pi_permission_mode     / PI_PERMISSION_MODE     auto_allow|deny|ask
- pi_busy_behavior       / PI_BUSY_BEHAVIOR       followUp|steer|reject
- pi_timeout             / PI_STREAM_TIMEOUT      300
- pi_approval_timeout    / PI_APPROVAL_TIMEOUT    120
"""

from .sidecar_handler import SidecarAgentHandler

__author__ = "bibow"

__all__ = ["PiAgentHandler"]


class PiAgentHandler(SidecarAgentHandler):
    """A2A handler bridging tasks to the pi sidecar (HTTP + SSE)."""

    CONFIG_PREFIX = "pi"
    ENV_PREFIX = "PI"
    SESSION_MAP_PREFIX = "pi_session"
    RESULT_KEY = "pi"
    AGENT_LABEL = "pi"
    DIALOG_METHODS = ("select", "confirm", "input", "editor")
    DEFAULT_API_URL = "http://localhost:3300"
