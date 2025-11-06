"""CLI para ejecutar un flujo grabado."""
from __future__ import annotations

import argparse

from optin_browser.playback import FlowPlaybackError, FlowPlayer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reproduce un flujo grabado con variables.")
    parser.add_argument("--alias", required=True, help="Nombre del flujo guardado")
    parser.add_argument("--account", help="Alias de cuenta para reutilizar sesión si aplica")
    parser.add_argument("--var", action="append", default=[], help="Variables en formato CLAVE=VALOR")
    return parser.parse_args()


def build_variables(pairs: list[str]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Variable inválida: {pair}")
        key, value = pair.split("=", 1)
        variables[key] = value
    return variables


def main() -> None:
    args = parse_args()
    try:
        player = FlowPlayer(args.alias, build_variables(args.var))
        player.run(account=args.account)
        print("Flujo ejecutado con éxito.")
    except (FlowPlaybackError, ValueError) as exc:
        print(f"Error durante la reproducción: {exc}")


if __name__ == "__main__":
    main()
