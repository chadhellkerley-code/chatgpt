# tools/build_executable.py
# -*- coding: utf-8 -*-
"""Utilidad para generar ejecutables por licencia."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Tuple


def _slugify(value: str) -> str:
    cleaned = [c if c.isalnum() or c in {"_", "-"} else "_" for c in value.lower()]
    slug = "".join(cleaned).strip("_")
    return slug or "cliente"


def _guess_output(dist_dir: Path, name: str) -> Path:
    if sys.platform.startswith("win"):
        candidate = dist_dir / f"{name}.exe"
        if candidate.exists():
            return candidate
    elif sys.platform == "darwin":
        candidate = dist_dir / f"{name}.app"
        if candidate.exists():
            return candidate
    candidate = dist_dir / name
    if candidate.exists():
        return candidate
    # fallback al .exe por si PyInstaller usa sufijo aun en otros SO
    candidate_exe = dist_dir / f"{name}.exe"
    if candidate_exe.exists():
        return candidate_exe
    return candidate


def _copy_project(src: Path, dest: Path) -> None:
    ignore = shutil.ignore_patterns(
        "venv*",
        "dist",
        "build",
        "__pycache__",
        "*.pyc",
        "*.pyo",
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        "*.log",
    )
    shutil.copytree(src, dest, ignore=ignore)


def _sanitize_tree(root: Path) -> None:
    for name in (".env", ".env.local"):
        target = root / name
        if target.exists():
            target.unlink()

    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    for item in data_dir.glob("*"):
        if item.is_file():
            item.unlink()

    leads_dir = root / "text" / "leads"
    leads_dir.mkdir(parents=True, exist_ok=True)
    for item in leads_dir.glob("*"):
        if item.is_file():
            item.unlink()

    storage_dir = root / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    for item in storage_dir.glob("*.json*"):
        item.unlink()
    logs_dir = storage_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for item in logs_dir.glob("*"):
        if item.is_file():
            item.unlink()


def _write_client_env(root: Path) -> None:
    env_path = root / ".env"
    env_path.write_text(
        "CLIENT_DISTRIBUTION=1\n# Configurá tus propias claves aquí\n",
        encoding="utf-8",
    )


_HIDDEN_IMPORTS = [
    "accounts",
    "actions.hashtag_mode",
    "app",
    "config",
    "ig",
    "leads",
    "licensekit",
    "proxy_manager",
    "responder",
    "runtime",
    "session_store",
    "state_view",
    "storage",
    "totp_store",
    "ui",
    "utils",
]


def build_for_license(
    record: Dict[str, str], *, name: str | None = None
) -> Tuple[bool, Path | None, str]:
    """Genera un ejecutable para la licencia suministrada."""

    root = Path(__file__).resolve().parents[1]
    launcher = root / "client_launcher.py"
    if not launcher.exists():
        return False, None, "No se encontró client_launcher.py"

    dist_dir = root / "dist"
    dist_dir.mkdir(exist_ok=True)

    exe_name = name or f"insta_cli_{_slugify(record.get('client_name') or 'cliente')}"

    temp_base = Path(tempfile.mkdtemp(prefix="license_build_"))
    workspace = temp_base / "workspace"

    try:
        _copy_project(root, workspace)
        _sanitize_tree(workspace)
        _write_client_env(workspace)

        payload_path = workspace / "storage" / "license_payload.json"
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "license_key": record.get("license_key"),
            "client_name": record.get("client_name"),
            "expires_at": record.get("expires_at"),
            "status": record.get("status", "active"),
        }
        payload_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        bundle_base = dist_dir / f"{exe_name}_source"
        if bundle_base.with_suffix(".zip").exists():
            bundle_base.with_suffix(".zip").unlink()
        archive_path = Path(
            shutil.make_archive(str(bundle_base), "zip", root_dir=workspace)
        )

        command = [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--onefile",
            "--name",
            exe_name,
            "--add-data",
            f"{payload_path}{os.pathsep}storage",
            "client_launcher.py",
        ]

        for module in _HIDDEN_IMPORTS:
            command.extend(["--hidden-import", module])

        subprocess.run(command, check=True, cwd=workspace)

        output = _guess_output(workspace / "dist", exe_name)
        if not output.exists():
            return False, None, "PyInstaller no generó el archivo esperado."

        final_output = dist_dir / output.name
        if final_output.exists():
            final_output.unlink()
        shutil.move(str(output), final_output)
        message = (
            f"Ejecutable generado en {final_output} (bundle limpio: {archive_path})"
        )
        return True, final_output, message
    except subprocess.CalledProcessError as exc:
        return False, None, f"Error al ejecutar PyInstaller: {exc}"
    finally:
        shutil.rmtree(temp_base, ignore_errors=True)

