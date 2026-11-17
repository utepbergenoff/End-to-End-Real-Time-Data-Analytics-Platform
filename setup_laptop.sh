#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.docker/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${PYTHON_BIN:-$root/../venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
  echo "Parent virtual environment not found at: $python_bin" >&2
  echo "Set PYTHON_BIN to its Python executable, or place it at ../venv/bin/python." >&2
  exit 1
fi

"$python_bin" -c 'import sys; assert sys.version_info[:2] == (3, 14), f"Python 3.14 required; found {sys.version}"'
echo "Using existing Python: $python_bin"
command -v docker >/dev/null || { echo "Docker Desktop CLI not found in PATH" >&2; exit 1; }
docker info >/dev/null || { echo "Start Docker Desktop first" >&2; exit 1; }
docker compose -f "$root/docker-compose.yml" config --quiet
"$python_bin" -m pip install -r "$root/requirements.txt"
"$python_bin" -m pip check
(cd "$root" && "$python_bin" -m unittest -v test_core.py)
echo "Laptop setup verified. Run: bash run_demo.sh"
