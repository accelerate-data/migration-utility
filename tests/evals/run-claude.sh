#!/usr/bin/env bash
# Promptfoo exec provider wrapper.
# Receives: $1 = rendered prompt, $2 = options JSON, $3 = context JSON
#
# Uses --output-format text so promptfoo gets the raw agent/skill output
# as the 'output' variable in assertions. Agent scenarios print their
# output JSON to stdout; skill scenarios print human-formatted text.
set -euo pipefail

prompt="$1"

claude -p \
  --output-format text \
  --allowedTools "Read,Write,Bash" \
  --max-turns 30 \
  --dangerously-skip-permissions \
  "$prompt"
