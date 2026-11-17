#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.docker/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${PYTHON_BIN:-$root/../.venv/bin/python}"
pid_file="$root/data/demo-pids.txt"
[[ -x "$python_bin" ]] || { echo "Python not found: $python_bin" >&2; exit 1; }
command -v docker >/dev/null || { echo "Docker Desktop CLI not found" >&2; exit 1; }
docker info >/dev/null || { echo "Start Docker Desktop first" >&2; exit 1; }
mkdir -p "$root/data" "$root/artifacts"

if [[ -f "$pid_file" ]]; then
  while IFS= read -r pid; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "Demo processes are already running. Use bash stop_demo.sh first." >&2
      exit 1
    fi
  done < "$pid_file"
fi
: > "$pid_file"
cd "$root"

"$python_bin" app.py train
"$python_bin" app.py evaluate
docker compose up -d broker
ready=0
for _ in {1..30}; do
  if "$python_bin" app.py setup-topics >/dev/null 2>&1; then ready=1; break; fi
  sleep 2
done
[[ "$ready" -eq 1 ]] || { echo "Kafka did not become ready. Check docker compose logs broker." >&2; exit 1; }

start_worker() {
  local command="$1" index="$2"
  nohup "$python_bin" app.py "$command" >"$root/data/$command-$index.out.log" 2>"$root/data/$command-$index.err.log" </dev/null &
  echo "$!" >> "$pid_file"
}
start_worker storage 0
start_worker processor 1
start_worker processor 2
start_worker processor 3
start_worker dashboard 4
docker compose up -d spark
sleep 8
"$python_bin" app.py produce --minutes 240 --rate 30
echo "Dashboard: http://127.0.0.1:8000"
echo "Benchmark: \"$python_bin\" app.py benchmark"
echo "Stop: bash stop_demo.sh"
