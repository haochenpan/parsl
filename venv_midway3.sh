#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="./venv"
VENV_ALIAS="parsl-midway3-$(date +%Y%m%d)"
PYTHON_MODULE="python/3.11.9"
PYTHON_BIN="python3"

# # Load modules init script (if needed), then load a Midway3 Python module.
# if ! command -v module >/dev/null 2>&1 && [ -f /etc/profile.d/modules.sh ]; then
#   # shellcheck source=/etc/profile.d/modules.sh
#   source /etc/profile.d/modules.sh
# fi
if command -v module >/dev/null 2>&1; then
  module load "$PYTHON_MODULE"
fi

rm -rf "$VENV_DIR"
"$PYTHON_BIN" -m venv "$VENV_DIR" --prompt "$VENV_ALIAS"
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip

python -m pip install -e '.[diaspora,monitoring]'

echo "Venv ready at: $VENV_DIR"
echo "Installed package: parsl (editable) with extras: diaspora, monitoring"
