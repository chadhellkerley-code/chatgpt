# storage.py
# -*- coding: utf-8 -*-
import csv
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from zoneinfo import ZoneInfo

from config import SETTINGS, read_env_local, update_env_local
from ui import Fore, banner, full_line, style_text
from utils import ask, ok, press_enter, warn

BASE = Path(__file__).resolve().parent
STO = BASE / "storage"
STO.mkdir(exist_ok=True)
SENT = STO / "sent_log.jsonl"
AUTO = STO / "autoresponder_state.json"
STATE = STO / "state.json"
EXPORTS = STO / "exports"
EXPORTS.mkdir(exist_ok=True)

TZ_LABEL = "America/Argentina/Cordoba"
try:
    TZ = ZoneInfo(TZ_LABEL)
except Exception:  # pragma: no cover - fallback si falta la zona
    TZ = ZoneInfo("UTC")


def _now_local() -> datetime:
    return datetime.now(TZ)


def _start_of_day(dt: datetime) -> datetime:
    return datetime.combine(dt.date(), dtime.min, tzinfo=TZ)


def _load_state() -> dict:
    if not STATE.exists():
        return {
            "last_daily_reset": None,
            "daily_sent": 0,
            "daily_errors": 0,
            "last_reset_display": None,
        }
    try:
        data = json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    data.setdefault("last_daily_reset", None)
    data.setdefault("daily_sent", 0)
    data.setdefault("daily_errors", 0)
    data.setdefault("last_reset_display", None)
    return data


def _save_state(state: dict) -> None:
    try:
        STATE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _iter_records() -> Iterator[dict]:
    if not SENT.exists():
        return iter(())
    def _generator() -> Iterator[dict]:
        for line in SENT.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            ts = obj.get("ts")
            if ts is None:
                continue
            try:
                dt = datetime.fromtimestamp(float(ts), tz=ZoneInfo("UTC")).astimezone(TZ)
            except Exception:
                continue
            obj["local_dt"] = dt
            yield obj
    return _generator()


def _status_from_record(entry: dict) -> str:
    detail = (entry.get("detail") or "").strip()
    if detail:
        return detail
    return "Mensaje enviado" if entry.get("ok") else "Error"


def conversation_rows(
    *,
    account_filter: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> List[dict]:
    """Devuelve una vista plana del estado de conversaciones recientes."""

    account_key = account_filter.lower().lstrip("@") if account_filter else None
    latest: dict[tuple[str, str], dict] = {}
    for entry in _iter_records():
        local_dt: datetime | None = entry.get("local_dt")
        if not local_dt:
            continue
        if start and local_dt < start:
            continue
        if end and local_dt > end:
            continue
        account = str(entry.get("account", ""))
        if account_key and account.lower().lstrip("@") != account_key:
            continue
        recipient = str(entry.get("to", ""))
        key = (account, recipient)
        status = _status_from_record(entry)
        payload = {
            "timestamp": local_dt,
            "account": account,
            "recipient": recipient,
            "status": status,
        }
        previous = latest.get(key)
        if previous is None or previous["timestamp"] <= local_dt:
            latest[key] = payload
    rows = sorted(latest.values(), key=lambda item: item["timestamp"], reverse=True)
    return rows


def export_conversation_state(rows: List[dict]) -> Path:
    """Exporta el estado de conversaciones a un CSV dentro de storage/exports."""

    timestamp = _now_local().strftime("%Y%m%d-%H%M%S")
    path = EXPORTS / f"conversation_state_{timestamp}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["fecha_hora", "cuenta_emisora", "cuenta_receptora", "estado"]
        )
        for row in rows:
            ts = row["timestamp"].strftime("%Y-%m-%d %H:%M")
            writer.writerow([ts, row["account"], row["recipient"], row["status"]])
    return path


def purge_conversations_before(cutoff: datetime) -> int:
    """Elimina registros anteriores al cutoff (en zona local)."""

    if not SENT.exists():
        return 0

    try:
        cutoff_utc = cutoff.astimezone(ZoneInfo("UTC"))
    except Exception:
        cutoff_utc = cutoff

    kept_lines: List[str] = []
    removed = 0
    for line in SENT.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            kept_lines.append(raw)
            continue
        ts = obj.get("ts")
        if ts is None:
            kept_lines.append(raw)
            continue
        try:
            dt_utc = datetime.fromtimestamp(float(ts), tz=ZoneInfo("UTC"))
        except Exception:
            kept_lines.append(raw)
            continue
        if dt_utc < cutoff_utc:
            removed += 1
            continue
        kept_lines.append(raw)

    with SENT.open("w", encoding="utf-8") as fh:
        if kept_lines:
            fh.write("\n".join(kept_lines) + "\n")
        else:
            fh.write("")
    return removed


def _counts_for_date(date: datetime.date) -> tuple[int, int]:
    sent = 0
    errors = 0
    for obj in _iter_records():
        local_dt = obj.get("local_dt")
        if not local_dt or local_dt.date() != date:
            continue
        if obj.get("ok"):
            sent += 1
        else:
            errors += 1
    return sent, errors


def _ensure_state_today(state: dict) -> dict:
    today = _now_local().date()
    today_iso = today.isoformat()
    needs_reset = state.get("last_daily_reset") != today_iso or state.get("daily_sent") is None
    if needs_reset:
        sent_today, errors_today = _counts_for_date(today)
        state["last_daily_reset"] = today_iso
        state["daily_sent"] = sent_today
        state["daily_errors"] = errors_today
        state["last_reset_display"] = _start_of_day(_now_local()).strftime("%Y-%m-%d %H:%M")
        _save_state(state)
    return state


def _increment_daily(okflag: bool) -> None:
    state = _ensure_state_today(_load_state())
    if okflag:
        state["daily_sent"] = int(state.get("daily_sent", 0)) + 1
    else:
        state["daily_errors"] = int(state.get("daily_errors", 0)) + 1
    state.setdefault("last_reset_display", _start_of_day(_now_local()).strftime("%Y-%m-%d %H:%M"))
    _save_state(state)


def log_sent(account: str, username: str, okflag: bool, detail: str = ""):
    rec = {
        "ts": int(time.time()),
        "account": account,
        "to": username,
        "ok": bool(okflag),
        "detail": detail,
    }
    with SENT.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    _increment_daily(bool(okflag))


def already_contacted(username: str) -> bool:
    if not SENT.exists():
        return False
    for line in SENT.read_text(encoding="utf-8").splitlines():
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("to", "").lower() == username.lower():
            return True
    return False


def sent_totals() -> tuple[int, int]:
    """Devuelve totales acumulados de envíos OK y con error."""
    ok_count = 0
    error_count = 0
    for obj in _iter_records():
        if obj.get("ok"):
            ok_count += 1
        else:
            error_count += 1
    return ok_count, error_count


def sent_totals_today() -> tuple[int, int, str, str]:
    state = _ensure_state_today(_load_state())
    sent_today = int(state.get("daily_sent", 0))
    errors_today = int(state.get("daily_errors", 0))
    last_reset = state.get("last_reset_display") or _start_of_day(_now_local()).strftime(
        "%Y-%m-%d %H:%M"
    )
    return sent_today, errors_today, last_reset, TZ_LABEL


def _records_list() -> list[dict]:
    return list(_iter_records())


def _print_records(records: Iterable[dict], limit: int | None = None) -> None:
    shown = 0
    for obj in records:
        if limit is not None and shown >= limit:
            break
        ts = obj.get("local_dt") or datetime.fromtimestamp(obj.get("ts", 0), tz=ZoneInfo("UTC")).astimezone(TZ)
        status = "OK" if obj.get("ok") else "ERROR"
        detail = obj.get("detail", "")
        timestamp = ts.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"{timestamp}  @{obj.get('account')} → @{obj.get('to')}  [{status}] {detail}"
        )
        shown += 1
    if shown == 0:
        print("(sin registros)")


def _filter_by_account(records: list[dict]) -> None:
    alias = ask("Alias/cuenta (vacío para cancelar): ").strip()
    if not alias:
        warn("Sin cambios.")
        press_enter()
        return
    filtered = [r for r in records if str(r.get("account", "")).lower() == alias.lower()]
    banner()
    print(style_text(f"Últimos envíos para @{alias}:", bold=True, color=Fore.CYAN))
    _print_records(reversed(filtered))
    press_enter()


def _filter_by_date(records: list[dict]) -> None:
    start_raw = ask("Desde (YYYY-MM-DD, vacío para cancelar): ").strip()
    if not start_raw:
        warn("Sin cambios.")
        press_enter()
        return
    end_raw = ask("Hasta (YYYY-MM-DD, vacío = hoy): ").strip()
    try:
        start_date = datetime.strptime(start_raw, "%Y-%m-%d").date()
    except ValueError:
        warn("Formato inválido. Use YYYY-MM-DD.")
        press_enter()
        return
    if end_raw:
        try:
            end_date = datetime.strptime(end_raw, "%Y-%m-%d").date()
        except ValueError:
            warn("Formato inválido en fecha final.")
            press_enter()
            return
    else:
        end_date = _now_local().date()
    if end_date < start_date:
        warn("La fecha final debe ser mayor o igual a la inicial.")
        press_enter()
        return
    filtered = []
    for rec in records:
        local_dt = rec.get("local_dt")
        if not local_dt:
            continue
        if start_date <= local_dt.date() <= end_date:
            filtered.append(rec)
    banner()
    print(
        style_text(
            f"Envíos entre {start_date} y {end_date}:", bold=True, color=Fore.CYAN
        )
    )
    _print_records(reversed(filtered))
    press_enter()


def _export_csv(records: list[dict]) -> None:
    if not records:
        warn("No hay registros para exportar.")
        press_enter()
        return
    path = STO / "sent_log.csv"
    with path.open("w", encoding="utf-8") as fh:
        fh.write("timestamp,account,to,status,detail\n")
        for rec in records:
            ts = rec.get("local_dt") or datetime.fromtimestamp(rec.get("ts", 0), tz=ZoneInfo("UTC")).astimezone(TZ)
            status = "OK" if rec.get("ok") else "ERROR"
            detail = str(rec.get("detail", "")).replace("\n", " ").replace(",", " ")
            fh.write(
                f"{ts.strftime('%Y-%m-%d %H:%M:%S')},{rec.get('account')},{rec.get('to')},{status},{detail}\n"
            )
    ok(f"Exportado a {path}")
    press_enter()


def _aggregate_stats(records: Iterable[dict], days: int) -> dict:
    now = _now_local()
    start_date = now.date() - timedelta(days=days - 1)
    start_dt = datetime.combine(start_date, dtime.min, tzinfo=TZ)
    totals = {
        "sent": 0,
        "errors": 0,
        "per_day": defaultdict(lambda: {"sent": 0, "errors": 0}),
        "per_account": Counter(),
    }
    for rec in records:
        local_dt = rec.get("local_dt")
        if not local_dt or local_dt < start_dt:
            continue
        date_key = local_dt.date()
        if rec.get("ok"):
            totals["sent"] += 1
            totals["per_day"][date_key]["sent"] += 1
            if rec.get("account"):
                totals["per_account"][rec["account"]] += 1
        else:
            totals["errors"] += 1
            totals["per_day"][date_key]["errors"] += 1
    return totals


def _render_stats(title: str, data: dict) -> None:
    print(full_line())
    print(style_text(title, color=Fore.CYAN, bold=True))
    print(full_line())
    print(
        style_text(
            f"Enviados: {data['sent']} | Errores: {data['errors']}",
            color=Fore.GREEN if data["sent"] else Fore.WHITE,
            bold=True,
        )
    )
    if data["per_day"]:
        print(style_text("Por día:", bold=True))
        for day in sorted(data["per_day"].keys()):
            item = data["per_day"][day]
            print(f"  {day}: enviados {item['sent']} | errores {item['errors']}")
    if data["per_account"]:
        print(style_text("Top cuentas:", bold=True))
        for idx, (account, count) in enumerate(data["per_account"].most_common(5), start=1):
            print(f"  {idx}) @{account} — {count}")
    else:
        print("(sin cuentas registradas)")
    print()


def _show_statistics(records: list[dict]) -> None:
    banner()
    if not records:
        print(style_text("Sin registros de envíos para calcular estadísticas.", color=Fore.YELLOW))
        press_enter()
        return
    print(style_text("📈 Estadísticas", color=Fore.CYAN, bold=True))
    daily = _aggregate_stats(records, 1)
    weekly = _aggregate_stats(records, 7)
    monthly = _aggregate_stats(records, 30)
    _render_stats("— Estadísticas (HOY) —", daily)
    _render_stats("— Últimos 7 días —", weekly)
    _render_stats("— Últimos 30 días —", monthly)
    press_enter()


def menu_logs():
    while True:
        records = _records_list()
        banner()
        print(style_text("📜 REGISTROS Y ESTADÍSTICAS", color=Fore.CYAN, bold=True))
        print(full_line())
        print("1) Ver últimos envíos")
        print("2) Filtrar por alias/cuenta")
        print("3) Filtrar por rango de fechas")
        print("4) 📈 Ver estadísticas (Diarias / Semanales / Mensuales)")
        print("5) Exportar CSV")
        print("6) Volver")
        print()
        choice = ask("Opción: ").strip()
        if choice == "1":
            banner()
            print(style_text("Últimos envíos:", bold=True, color=Fore.CYAN))
            _print_records(records[-50:])
            press_enter()
        elif choice == "2":
            _filter_by_account(records)
        elif choice == "3":
            _filter_by_date(records)
        elif choice == "4":
            _show_statistics(records)
        elif choice == "5":
            _export_csv(records)
        elif choice == "6":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def get_auto_state() -> dict:
    if not AUTO.exists():
        return {}
    try:
        return json.loads(AUTO.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_auto_state(state: dict):
    AUTO.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _current_supabase() -> tuple[str, str]:
    env_local = read_env_local()
    url = env_local.get("SUPABASE_URL") or SETTINGS.supabase_url or ""
    key = env_local.get("SUPABASE_KEY") or SETTINGS.supabase_key or ""
    return url, key


def _is_valid_url(url: str) -> bool:
    return bool(re.match(r"^https?://", url))


def menu_supabase() -> None:
    while True:
        banner()
        url, key = _current_supabase()
        print(full_line())
        print(style_text("Configuración de Supabase", color=Fore.CYAN, bold=True))
        print(full_line())
        print(f"URL actual: {url or '(sin definir)'}")
        masked = key[:4] + "…" if key else "(sin definir)"
        print(f"API Key: {masked}")
        print()
        print("1) Configurar SUPABASE_URL")
        print("2) Configurar SUPABASE_KEY")
        print("3) Probar conexión")
        print("4) Volver")
        print()
        choice = ask("Opción: ").strip()
        if choice == "1":
            new_url = ask("Nueva URL (dejar vacío para cancelar): ").strip()
            if not new_url:
                warn("Sin cambios.")
                press_enter()
                continue
            if not _is_valid_url(new_url):
                warn("La URL debe comenzar con http:// o https://")
                press_enter()
                continue
            update_env_local({"SUPABASE_URL": new_url})
            ok("SUPABASE_URL guardada en .env.local")
            press_enter()
        elif choice == "2":
            new_key = ask("Nueva SUPABASE_KEY (dejar vacío para cancelar): ").strip()
            if not new_key:
                warn("Sin cambios.")
                press_enter()
                continue
            update_env_local({"SUPABASE_KEY": new_key})
            ok("SUPABASE_KEY guardada en .env.local")
            press_enter()
        elif choice == "3":
            url, key = _current_supabase()
            if url and key:
                print(style_text("OK: valores configurados.", color=Fore.GREEN, bold=True))
            else:
                warn("Falta URL o KEY para probar conexión.")
            press_enter()
        elif choice == "4":
            break
        else:
            warn("Opción inválida.")
            press_enter()
