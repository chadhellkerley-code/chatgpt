"""Load account configuration for the opt-in tooling."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclass
class Account:
    alias: str
    username: str
    password: str
    totp_secret: Optional[str] = None
    proxy_url: Optional[str] = None
    user_agent: Optional[str] = None


def _normalize(row: Dict[str, str]) -> Account:
    return Account(
        alias=row.get("account", "").strip(),
        username=row.get("username", "").strip(),
        password=row.get("password", "").strip(),
        totp_secret=(row.get("totp_secret") or "").strip() or None,
        proxy_url=(row.get("proxy_url") or "").strip() or None,
        user_agent=(row.get("user_agent") or "").strip() or None,
    )


def _validate(account: Account) -> None:
    if not account.alias or not account.username or not account.password:
        raise ValueError("Account rows must include account, username and password columns")


def _read_csv(path: Path) -> List[Account]:
    accounts: List[Account] = []
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            account = _normalize(row)
            _validate(account)
            accounts.append(account)
    return accounts


def _read_json(path: Path) -> List[Account]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    accounts: List[Account] = []
    if isinstance(payload, dict):
        payload = payload.get("accounts", [])
    for row in payload:
        account = _normalize(row)
        _validate(account)
        accounts.append(account)
    return accounts


def load_accounts(path: Path = Path("data/accounts.csv")) -> List[Account]:
    if not path.exists():
        raise FileNotFoundError(f"Accounts file not found: {path}")
    if path.suffix.lower() == ".json":
        return _read_json(path)
    return _read_csv(path)
