#!/usr/bin/env bash
# Lists Makefile targets documented with "## <text>". Invoked as: bash scripts/help.sh
set -euo pipefail
makefile="${1:-Makefile}"
grep -E '^[a-zA-Z_-]+:.*## ' "$makefile" \
  | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $1, $2}'
