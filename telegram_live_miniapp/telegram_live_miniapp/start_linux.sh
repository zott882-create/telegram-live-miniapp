#!/usr/bin/env sh
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8080}"
export HOST PORT
python3 app.py
