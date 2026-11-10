#!/usr/bin/python
"""
omp Agent Handler — A2A Bridge to the omp (oh-my-pi) sidecar service

``omp`` (https://github.com/can1357/oh-my-pi) is the coding agent that the
bundled sidecar in ``docker-a2a-gateway/pi/`` drives over the official RPC
protocol (``omp --mode rpc``, stdin/stdout JSONL). The sidecar exposes the
same HTTP + SSE surface as the pi sidecar, so this handler is the **omp
dialect** of the shared sidecar bridge (``handlers/sidecar_handler.py``):
every agent-specific value (metadata keys, setting keys, env prefix,
session-map prefix, reply metadata key) is a class attribute of
``SidecarAgentHandler`` and only the vocabulary is declared here.

What differs from pi:

- **Config keys** — ``omp_api_url`` / ``OMP_API_URL`` … (``omp_*`` metadata,
  ``OMP_*`` setting/env). The sidecar default is ``http://localhost:3310``.
- **Session map** — ``omp_session#{context_key}`` in ``a2a_settings``
  (partition-scoped, like the pi bridge).
- **Reply metadata** — ``metadata.omp`` (sessionId, sessionFile, aborted,
  stopReason, usage, model) and ``metadata.omp_error``.
- **Thinking levels** — omp's own set (``inherit``…``max``); an unknown
  level is dropped (the sidecar default applies).
- **Approvals** — omp gates its tools natively (``--approval-mode
  always-ask``); the prompt arrives as an ``extension_ui_request`` with
  ``method: "select"`` and the options ``Approve`` / ``Approve <tool> for
  this session`` / ``Deny``. The labels are matched against
  ``omp_approval_allow_labels`` / ``omp_approval_deny_labels`` (comma lists,
  case-insensitive) and against the conventional allow/deny words. When no
  option can be identified the dialog is cancelled — the bridge never guesses
  an approval.

The provider failure arrives as the sidecar's ``run_end.error`` (omp's
``prompt_result.status: "error"``) and an aborted run as ``run_end.aborted``
— both are handled by the inherited result assembly (task FAILED / CANCELED).
"""

from __future__ import annotations

import re
from typing import Any

from .sidecar_handler import SidecarAgentHandler

__author__ = "bibow"

__all__ = ["OmpAgentHandler"]

# Conventional approval words — the first option matching one of them is
# used when the operator did not configure explicit label lists.
_ALLOW_PATTERN = re.compile(r"^(approve|allow|yes|ok)\b", re.IGNORECASE)
_DENY_PATTERN = re.compile(r"^(deny|no|cancel|reject)\b", re.IGNORECASE)



class OmpAgentHandler(SidecarAgentHandler):
    """A2A handler bridging tasks to the omp sidecar (HTTP + SSE)."""

    CONFIG_PREFIX = "omp"
    ENV_PREFIX = "OMP"
    SESSION_MAP_PREFIX = "omp_session"
    RESULT_KEY = "omp"
    AGENT_LABEL = "omp"
    DIALOG_METHODS = ("select", "confirm", "input", "editor", "ask")
    DEFAULT_API_URL = "http://localhost:3310"

    # omp's thinking levels (upstream ThinkingLevel). "inherit" is not a CLI
    # value — the sidecar omits the flag for it.
    THINKING_LEVELS = (
        "inherit", "off", "minimal", "low", "medium", "high", "xhigh", "max",
    )

    # The shared option table plus omp's approval label lists. The api_url
    # default is the dialect's DEFAULT_API_URL (the base resolves a None
    # default to it).
    OPTION_SPECS = SidecarAgentHandler.OPTION_SPECS + (
        ("approval_allow_labels", "approval_allow_labels",
         "APPROVAL_ALLOW_LABELS", "approval_allow_labels", ""),
        ("approval_deny_labels", "approval_deny_labels",
         "APPROVAL_DENY_LABELS", "approval_deny_labels", ""),
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # The approval label lists are comma separated strings.
        self.approval_allow_labels = str(self.approval_allow_labels or "")
        self.approval_deny_labels = str(self.approval_deny_labels or "")
        # An unknown thinking level is not forwarded — the sidecar default
        # applies instead (pi forwards its level unvalidated).
        if self.thinking and self.thinking not in self.THINKING_LEVELS:
            self.logger.warning(
                "%s thinking level %r is not supported (allowed: %s); "
                "using the sidecar default",
                self.AGENT_LABEL, self.thinking,
                ", ".join(self.THINKING_LEVELS),
            )
            self.thinking = ""

    # ------------------------------------------------------------------
    # Approval policy (omp's native tool approval)
    # ------------------------------------------------------------------

    def _label_list(self, raw: str) -> list[str]:
        """Split a comma-separated label list into lower-cased entries."""
        return [part.strip().lower() for part in (raw or "").split(",")
                if part.strip()]

    def _find_option(
        self, options: list[Any], labels: list[str], pattern: re.Pattern[str]
    ) -> str | None:
        """The first option matching a configured label or the conventional
        word pattern (case-insensitive). ``None`` when nothing matches."""
        for option in options:
            if not isinstance(option, str):
                continue
            lowered = option.lower()
            if lowered in labels:
                return option
            if pattern.match(option.strip()):
                return option
        return None

    def _allow_option(self, event: dict[str, Any]) -> str | None:
        options = event.get("options")
        if not isinstance(options, list):
            return None
        return self._find_option(
            options,
            self._label_list(self.approval_allow_labels),
            _ALLOW_PATTERN,
        )

    def _deny_option(self, event: dict[str, Any]) -> str | None:
        options = event.get("options")
        if not isinstance(options, list):
            return None
        return self._find_option(
            options,
            self._label_list(self.approval_deny_labels),
            _DENY_PATTERN,
        )

    def _policy_answer(
        self, event: dict[str, Any], mode: str
    ) -> dict[str, Any]:
        """The answer for omp's approval dialog (an ``select`` request).

        omp's native approval offers ``Approve`` / ``Approve <tool> for this
        session`` / ``Deny``. ``auto_allow`` picks the allow option,
        ``deny`` the deny option; a dialog whose options cannot be classified
        is cancelled (fail closed — the bridge never approves blindly).
        """
        if event.get("method") != "select":
            return super()._policy_answer(event, mode)
        if mode == "deny":
            deny_option = self._deny_option(event)
            if deny_option is None:
                return {"cancelled": True}
            return {"value": deny_option}
        allow_option = self._allow_option(event)
        if allow_option is None:
            return {"cancelled": True}
        return {"value": allow_option}

    def _deny_answer(self, event: dict[str, Any]) -> dict[str, Any]:
        """The answer for a denied / expired omp approval (fail closed)."""
        if event.get("method") != "select":
            return super()._deny_answer(event)
        deny_option = self._deny_option(event)
        if deny_option is None:
            return {"cancelled": True}
        return {"value": deny_option}
