#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export API_FOOTBALL_DEMO="${API_FOOTBALL_DEMO:-0}"
python combined_app.py
