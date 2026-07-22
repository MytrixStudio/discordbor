# Render bot

Este servicio contiene el bot de Discord y una cola HTTP. El mod de Minecraft inicia conexiones salientes hacia Render; no se abre ningún puerto en el servidor Minecraft.

## Render

1. Sube esta carpeta a GitHub.
2. En Render crea un **Web Service** desde el repositorio.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python main.py`
5. Health check: `/health`
6. Añade `DISCORD_TOKEN` y `BRIDGE_SECRET` como secretos.
7. Copia la URL pública generada, por ejemplo `https://mystrix-whitelist-bot.onrender.com`.

No existe `MINECRAFT_API_URL` en esta arquitectura.
