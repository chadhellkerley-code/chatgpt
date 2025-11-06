"""CLI para envío concurrente de DMs usando el toolkit opt-in."""
from __future__ import annotations

import argparse
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

from tenacity import retry, stop_after_attempt, wait_exponential

from optin_browser import audit
from optin_browser.config import cfg
from optin_browser.dm import DirectMessageError, send_dm
from optin_browser.utils import bounded_sleep

Row = Dict[str, str]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20), reraise=True)
def _send_with_retry(account: str, to_username: str, text: str) -> None:
    send_dm(account, to_username, text)


def _load_csv(path: Path) -> List[Row]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"to_username", "text"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Faltan columnas en el CSV: {', '.join(sorted(missing))}")
        return [dict(row) for row in reader]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Envía mensajes de forma concurrente.")
    parser.add_argument("--account", required=True, help="Alias local de la cuenta")
    parser.add_argument("--csv", required=True, help="Ruta del CSV con columnas to_username,text")
    parser.add_argument(
        "--parallel",
        type=int,
        help="Cantidad de hilos concurrentes (default OPTIN_PARALLEL_LIMIT)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    rows = _load_csv(csv_path)
    parallel = args.parallel or cfg.parallel_limit
    audit.log_event("bulk_send_start", account=args.account, total=len(rows), parallel=parallel)

    lock = threading.Lock()
    stats = {"sent": 0, "failed": 0}

    def task(row: Row) -> None:
        try:
            _send_with_retry(args.account, row["to_username"], row["text"])
            with lock:
                stats["sent"] += 1
            bounded_sleep(cfg.send_cooldown_seconds)
        except (DirectMessageError, Exception) as exc:  # noqa: BLE001
            with lock:
                stats["failed"] += 1
            audit.log_event(
                "bulk_send_failed",
                account=args.account,
                to=row.get("to_username"),
                reason=str(exc),
            )

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = [executor.submit(task, row) for row in rows]
        for future in as_completed(futures):
            future.result()

    audit.log_event("bulk_send_complete", account=args.account, **stats)
    print(f"Envíos exitosos: {stats['sent']}, fallidos: {stats['failed']}")


if __name__ == "__main__":
    main()
