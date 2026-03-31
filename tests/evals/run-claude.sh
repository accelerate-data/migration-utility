#!/usr/bin/env bash
# Promptfoo exec provider wrapper.
# Receives: $1 = rendered prompt, $2 = options JSON, $3 = context JSON
set -euo pipefail

prompt="$1"

claude -p \
  --output-format json \
  --allowedTools "Read,Write,Bash" \
  --max-turns 30 \
  --dangerously-skip-permissions \
  "$prompt"
