#!/bin/bash
# Lanzador rápido para macOS: doble click para abrir la herramienta.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -d "venv311" ]; then
  echo "No se encontró el entorno virtual venv311. Ejecutá ./setup_mac.sh primero."
  exit 1
fi

source "venv311/bin/activate"
python3 app.py
