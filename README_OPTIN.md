# Instagram Opt-in Toolkit

## Instalación

```bash
pip install -r requirements_optin.txt
python -m playwright install
```

## Variables de entorno

Crea un archivo `.env` con los siguientes valores de ejemplo:

```
OPTIN_HEADLESS=false
SESSION_ENCRYPTION_KEY=<FernetKeyBase64>
DELAY_MIN_S=190
DELAY_MAX_S=350
DM_PER_HOUR_LIMIT=25
GLOBAL_TIMEOUT_PER_STEP=15
RETRIES_PER_STEP=2
TWOFA_RESEND_COOLDOWN=90
MAX_CONSECUTIVE_ERRORS_PER_ACCOUNT=3
OPTIN_PROXY_URL=
OPTIN_IG_TOTP=
```

## CSVs

- `data/accounts.csv` → columnas: `account,username,password,totp_secret,proxy_url,user_agent`
- `data/recipients.csv` → columnas: `account,to_username,text`

## Uso rápido (single)

```bash
python scripts/run_optin_wizard.py --account cuenta1
python scripts/run_optin_send.py --account cuenta1 --to usuario --text "Hola!"
```

## Uso rápido (multi)

```bash
python scripts/run_optin_batch_send.py --accounts data/accounts.csv --recipients data/recipients.csv --text "Hola!" --max-concurrency 5
```

## Recomendaciones operativas

- Usa headful (más humano) y límites conservadores.
- Re-grabar solo si cambia la UI: wizard o subflow puntual.
- Revisa `logs/optin_audit.jsonl` y genera un resumen diario en CSV si se requiere.
