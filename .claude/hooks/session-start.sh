#!/bin/bash
set -euo pipefail

# Install dependencies for Claude Code on the web sessions so tests, ruff,
# and pyright work immediately. Local checkouts manage their own environment.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

if ! command -v uv >/dev/null 2>&1; then
  pip install uv
fi

uv sync --locked
