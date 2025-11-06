# Toolkit opt-in para Instagram (Playwright)

## Preparación en Windows (PowerShell)

```powershell
py -m venv venv311
venv311\Scripts\activate
pip install -r requirements_optin.txt
python -m playwright install
```

## Variables `.env` sugeridas

```env
OPTIN_HEADLESS=0
OPTIN_PROXY_URL=
OPTIN_IG_TOTP=
OPTIN_SEND_COOLDOWN_SECONDS=90
OPTIN_TYPING_MIN_MS=60
OPTIN_TYPING_MAX_MS=180
OPTIN_PARALLEL_LIMIT=3
SESSION_ENCRYPTION_KEY=<generar con Fernet>
OPTIN_USER_AGENT=
OPTIN_LOCALE=
OPTIN_TIMEZONE=
```

## Primer guardado de sesión

```bash
python scripts/run_optin_login.py --account main --user <ig_user> --password <ig_pass>
```

## Enviar un DM usando la sesión

```bash
python scripts/run_optin_send_dm.py --account main --to <dest> --text "Hola!"
```

## Grabar y reutilizar un flujo

```bash
python scripts/run_optin_record.py --alias add_account
python scripts/run_optin_playback.py --alias add_account --var USER=<...> --var PASSWORD=<...>
```

## Responder mensajes entrantes

```bash
python scripts/run_optin_reply_dm.py --account main --contains "palabra" --reply "Hola {username}!"
```

## Envío concurrente (CSV to_username,text)

```bash
python scripts/run_optin_bulk_send.py --account main --csv data/destinos.csv --parallel 5
```
