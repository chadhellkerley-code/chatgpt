"""CLI para grabar un flujo reutilizable."""
from __future__ import annotations

import argparse

from optin_browser.recorder import record_flow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Graba pasos manuales en Instagram para reutilizarlos.")
    parser.add_argument("--alias", required=True, help="Nombre del flujo a guardar")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    record_flow(args.alias)


if __name__ == "__main__":
    main()
