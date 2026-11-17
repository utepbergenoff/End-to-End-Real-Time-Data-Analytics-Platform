#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.docker/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pid_file="$root/data/demo-pids.txt"
if [[ -f "$pid_file" ]]; then
  while IFS= read -r pid; do
    [[ "$pid" =~ ^[0-9]+$ ]] && kill "$pid" 2>/dev/null || true
  done < "$pid_file"
  rm -f "$pid_file"
fi
cd "$root"
docker compose down
