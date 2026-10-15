#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Register the goose-agent fixture in the A2A gateway's PostgreSQL backend with
full metadata (module_name, class_name, goose_* connection details).

This script uses a direct SQL INSERT/UPDATE instead of GraphQL because the
silvaengine_utility JSON scalar has a compatibility issue with graphql-core
that prevents passing dict variables to GraphQL mutations (see
register_openclaw_agent.py — same approach).

Prerequisites:
    - PostgreSQL container running (a2a-postgres of docker-a2a-gateway)
    - A2A daemon / SilvaEngine Gateway running with db_backend=postgresql
    - `goose serve` running (default http://127.0.0.1:3284) with
      GOOSE_SERVER__SECRET_KEY set

Environment:
    GATEWAY_ENV      path to the docker-a2a-gateway .env (defaults to
                     ./docker-a2a-gateway/.env, or the gateway tests .env)
    PG_HOST / PG_PORT / PG_USER / PG_PASSWORD / PG_DB
    ENDPOINT_ID / PART_ID
    GOOSE_API_URL / GOOSE_CWD / GOOSE_PERMISSION_MODE
    GOOSE_AGENT_ID   (default "goose-agent")

Usage:
    python a2a_protocol_plugin/tests/register_goose_agent.py

Author: bibow
"""
from __future__ import print_function

__author__ = "bibow"

import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Load gateway .env for connection details
# ---------------------------------------------------------------------------
DEFAULT_ENV = str(
    Path(os.getenv("GATEWAY_ENV", "C:/Users/bibo7/gitrepo/docker-a2a-gateway/.env"))
)
# Fall back to the gateway test env used by the other register scripts.
FALLBACK_ENV = "C:/Users/bibo7/gitrepo/silvaengine/silvaengine_gateway/silvaengine_gateway/tests/.env"

env_path = Path(DEFAULT_ENV)
if not env_path.exists():
    env_path = Path(FALLBACK_ENV)

env = {}
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if " #" in value:
                value = value.split(" #", 1)[0].strip()
            if key:
                env[key] = value
else:
    print(f"WARNING: no env file found (tried {DEFAULT_ENV}, {FALLBACK_ENV})")

PG_HOST = env.get("PG_HOST", "localhost")
PG_PORT = env.get("PG_PORT", "5432")
PG_USER = env.get("PG_USER", "silvaengine")
PG_PASSWORD = env.get("PG_PASSWORD", "silvaengine")
PG_DB = env.get("PG_DB", "silvaengine")
ENDPOINT_ID = os.getenv("ENDPOINT_ID", env.get("endpoint_id", "a2a"))
PART_ID = os.getenv("PART_ID", env.get("part_id", "default"))
PARTITION_KEY = f"{ENDPOINT_ID}#{PART_ID}"

GOOSE_API_URL = os.getenv("GOOSE_API_URL", env.get("GOOSE_API_URL", "http://127.0.0.1:3284"))
GOOSE_CWD = os.getenv("GOOSE_CWD", env.get("GOOSE_CWD", "/workspace"))
GOOSE_PERMISSION_MODE = os.getenv(
    "GOOSE_PERMISSION_MODE", env.get("GOOSE_PERMISSION_MODE", "auto_allow"))
GOOSE_AGENT_ID = os.getenv("GOOSE_AGENT_ID", "goose-agent")
GATEWAY_PUBLIC_URL = os.getenv("GATEWAY_PUBLIC_URL", f"http://127.0.0.1:8765")

# NOTE: GOOSE_SECRET_KEY is deliberately NOT stored in the metadata record —
# the gateway's own env (GOOSE_SECRET_KEY) is preferred at resolve time.

# ---------------------------------------------------------------------------
# Register agent via direct SQL
# ---------------------------------------------------------------------------
METADATA = json.dumps({
    "module_name": "a2a_protocol_plugin.handlers.goose_handler",
    "class_name": "GooseAgentHandler",
    "goose_api_url": GOOSE_API_URL,
    "goose_cwd": GOOSE_CWD,
    "goose_permission_mode": GOOSE_PERMISSION_MODE,
    "goose_timeout": 300.0,
})

import psycopg2

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    user=PG_USER,
    password=PG_PASSWORD,
    dbname=PG_DB,
)

cur = conn.cursor()

# Check if agent exists
cur.execute(
    "SELECT agent_id FROM a2a_agents WHERE partition_key = %s AND agent_id = %s",
    (PARTITION_KEY, GOOSE_AGENT_ID),
)
exists = cur.fetchone()

if exists:
    cur.execute(
        """UPDATE a2a_agents
           SET metadata = %s,
               endpoint_url = %s,
               updated_at = NOW()
           WHERE partition_key = %s AND agent_id = %s""",
        (METADATA, GATEWAY_PUBLIC_URL, PARTITION_KEY, GOOSE_AGENT_ID),
    )
    print(f"Updated {GOOSE_AGENT_ID} with handler metadata")
else:
    cur.execute(
        """INSERT INTO a2a_agents
           (partition_key, agent_id, endpoint_id, part_id, agent_name,
            endpoint_url, status, metadata, updated_by, created_at, updated_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())""",
        (PARTITION_KEY, GOOSE_AGENT_ID, ENDPOINT_ID, PART_ID,
         "Goose Agent", GATEWAY_PUBLIC_URL, "active",
         METADATA, "goose-live"),
    )
    print(f"Inserted {GOOSE_AGENT_ID} with handler metadata")

conn.commit()

cur.execute(
    "SELECT agent_id, agent_name, metadata FROM a2a_agents WHERE partition_key = %s AND agent_id = %s",
    (PARTITION_KEY, GOOSE_AGENT_ID),
)
row = cur.fetchone()
print("\nVerified:")
print(f"  agent_id:  {row[0]}")
print(f"  agent_name: {row[1]}")
print(f"  metadata:  {row[2]}")

cur.close()
conn.close()