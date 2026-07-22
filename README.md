# Mystrix Render bridge

Servicio único para Render: levanta el bot de Discord y una cola HTTP. El mod de Minecraft no recibe conexiones públicas; solo sondea la URL de Render.

## Variables obligatorias en Render

- `DISCORD_TOKEN`: token secreto del bot desde Discord Developer Portal. No uses la URL de invitación, client secret, public key ni `Bot ...`.
- `DISCORD_APPLICATION_ID`: application/client ID del mismo bot.
- `DISCORD_GUILD_ID`: ID del servidor de Discord.
- `DISCORD_PANEL_CHANNEL_ID`: canal donde se publicará el panel.
- `DISCORD_ADMIN_ROLE_ID`: rol que puede ejecutar comandos administrativos.
- `BRIDGE_SECRET`: secreto largo y aleatorio compartido con el mod. Debe tener 32 caracteres o más.
- `BRIDGE_SERVER_ID`: mismo ID lógico que usará el mod, por defecto `mystrix-minecraft-1`.

Render configura `PORT` automáticamente. No configures `MINECRAFT_API_URL`; esta versión usa polling desde Minecraft hacia Render.

## Deploy en Render

1. Sube solo esta carpeta a GitHub.
2. Crea un **Web Service** en Render.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python main.py`
5. Health check path: `/health`
6. Añade `DISCORD_TOKEN` y `BRIDGE_SECRET` como secretos.

Si ves `Discord rechazó DISCORD_TOKEN`, el build está bien pero el token configurado en Render no es el token real del bot o fue regenerado/revocado.

## Config del mod

En el servidor Minecraft, configura:

- `MYSTRIX_BRIDGE_URL`: URL pública de Render, por ejemplo `https://mystrix-whitelist-bot.onrender.com`
- `MYSTRIX_BRIDGE_SECRET`: exactamente el mismo valor que `BRIDGE_SECRET`
- `MYSTRIX_BRIDGE_SERVER_ID`: exactamente el mismo valor que `BRIDGE_SERVER_ID`

También puedes poner esos valores en `config/whitelistbotdiscoerd-server.toml` si no usas variables de entorno.
