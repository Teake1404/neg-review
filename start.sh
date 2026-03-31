#!/bin/bash
set -e

cd /home/runner/workspace

export PORT="${PORT:-3000}"

exec python main.py
