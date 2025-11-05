# chatgpt

## Configuración de 2FA y sesiones

La automatización permite completar desafíos 2FA de forma automática o manual. Para ajustar el comportamiento incorpora las siguientes variables de entorno:

| Variable | Descripción | Valor por defecto |
| --- | --- | --- |
| `PROMPT_2FA_SMS` | Habilita el prompt CLI para ingresar códigos recibidos por SMS/WhatsApp cuando no hay TOTP disponible. | `true` |
| `PROMPT_2FA_TIMEOUT_SECONDS` | Tiempo máximo de espera para introducir el código manual antes de cancelar el inicio de sesión. | `180` |
| `SESSION_ENCRYPTION_KEY` | Clave utilizada para cifrar los archivos de sesión guardados. Si no se define, las sesiones se almacenan en texto plano. | *(vacío)* |

> **Nota:** cuando `SESSION_ENCRYPTION_KEY` está presente todos los archivos de sesión se guardan cifrados. Los procesos externos que necesiten reutilizar la sesión deben cargarla a través de `session_store.load_into` para que se aplique el descifrado automáticamente.
