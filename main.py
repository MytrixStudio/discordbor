"""Mystrix whitelist bridge: Discord bot + Render web service in one file."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hmac
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import time
from typing import Any
import uuid

from aiohttp import web
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

BOT_VERSION = "2.0.0"
PANEL_TITLE = "🔐 Registro de nicknames"
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{3,16}$")
ALLOWED_ACTIONS = {"ADD", "EDIT", "UNLINK", "LOOKUP"}
LOGGER = logging.getLogger("mystrix-render-bridge")


@dataclass(frozen=True, slots=True)
class Config:
    discord_token: str
    application_id: int
    guild_id: int
    panel_channel_id: int
    admin_role_id: int
    bridge_secret: str
    bridge_server_id: str
    web_host: str
    web_port: int
    operation_timeout_seconds: int
    operation_ttl_seconds: int
    lease_seconds: int
    heartbeat_timeout_seconds: int
    max_pending_operations: int

    @classmethod
    def load(cls) -> "Config":
        load_dotenv()

        def required(name: str) -> str:
            value = os.getenv(name, "").strip()
            if not value:
                raise RuntimeError(f"Falta la variable obligatoria {name}.")
            return value

        def snowflake(name: str) -> int:
            value = required(name)
            if not value.isdigit() or int(value) <= 0:
                raise RuntimeError(f"{name} debe ser un ID numérico válido.")
            return int(value)

        try:
            web_port = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")))
            timeout = int(os.getenv("OPERATION_TIMEOUT_SECONDS", "60"))
            ttl = int(os.getenv("OPERATION_TTL_SECONDS", "120"))
            lease = int(os.getenv("OPERATION_LEASE_SECONDS", "30"))
            heartbeat = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))
            max_pending = int(os.getenv("MAX_PENDING_OPERATIONS", "200"))
        except ValueError as exc:
            raise RuntimeError("Las variables numéricas contienen un valor inválido.") from exc

        if not 1 <= web_port <= 65535:
            raise RuntimeError("PORT debe estar entre 1 y 65535.")
        if timeout < 5 or ttl <= timeout or lease < 5 or heartbeat < 5:
            raise RuntimeError(
                "Revisa OPERATION_TIMEOUT_SECONDS, OPERATION_TTL_SECONDS, "
                "OPERATION_LEASE_SECONDS y HEARTBEAT_TIMEOUT_SECONDS."
            )
        if max_pending < 1:
            raise RuntimeError("MAX_PENDING_OPERATIONS debe ser mayor que cero.")

        return cls(
            discord_token=required("DISCORD_TOKEN"),
            application_id=snowflake("DISCORD_APPLICATION_ID"),
            guild_id=snowflake("DISCORD_GUILD_ID"),
            panel_channel_id=snowflake("DISCORD_PANEL_CHANNEL_ID"),
            admin_role_id=snowflake("DISCORD_ADMIN_ROLE_ID"),
            bridge_secret=required("BRIDGE_SECRET"),
            bridge_server_id=os.getenv("BRIDGE_SERVER_ID", "mystrix-minecraft-1").strip()
            or "mystrix-minecraft-1",
            web_host=os.getenv("WEB_HOST", "0.0.0.0").strip() or "0.0.0.0",
            web_port=web_port,
            operation_timeout_seconds=timeout,
            operation_ttl_seconds=ttl,
            lease_seconds=lease,
            heartbeat_timeout_seconds=heartbeat,
            max_pending_operations=max_pending,
        )


def configure_logging() -> None:
    Path("logs").mkdir(exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        "logs/mystrix-render-bridge.log",
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(console)
    root.addHandler(file_handler)


@dataclass(frozen=True, slots=True)
class BridgeResult:
    success: bool
    code: str
    message: str
    username: str | None = None
    old_username: str | None = None


@dataclass(slots=True)
class PendingOperation:
    operation_id: str
    action: str
    discord_id: int
    username: str | None
    created_at: float
    expires_at: float
    future: asyncio.Future[BridgeResult] = field(repr=False)
    leased_until: float = 0.0
    attempts: int = 0

    def public_payload(self) -> dict[str, Any]:
        return {
            "id": self.operation_id,
            "action": self.action,
            "discord_id": str(self.discord_id),
            "username": self.username,
            "created_at": int(self.created_at),
            "expires_at": int(self.expires_at),
            "attempt": self.attempts,
        }


class OperationBroker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._operations: dict[str, PendingOperation] = {}
        self._lock = asyncio.Lock()
        self.last_heartbeat_at: float | None = None

    async def submit(
        self,
        action: str,
        discord_id: int,
        username: str | None = None,
    ) -> BridgeResult:
        action = action.upper()
        if action not in ALLOWED_ACTIONS:
            return BridgeResult(False, "INVALID_ACTION", "Acción no permitida.")

        loop = asyncio.get_running_loop()
        now = time.time()
        operation = PendingOperation(
            operation_id=str(uuid.uuid4()),
            action=action,
            discord_id=discord_id,
            username=username.strip() if username else None,
            created_at=now,
            expires_at=now + self.config.operation_ttl_seconds,
            future=loop.create_future(),
        )

        async with self._lock:
            self._cleanup_locked(now)
            if len(self._operations) >= self.config.max_pending_operations:
                return BridgeResult(
                    False,
                    "QUEUE_FULL",
                    "El puente está ocupado. Inténtalo nuevamente en unos segundos.",
                )
            self._operations[operation.operation_id] = operation

        LOGGER.info(
            "Operación creada id=%s action=%s discord_id=%s",
            operation.operation_id,
            operation.action,
            operation.discord_id,
        )

        try:
            return await asyncio.wait_for(
                asyncio.shield(operation.future),
                timeout=self.config.operation_timeout_seconds,
            )
        except asyncio.TimeoutError:
            async with self._lock:
                current = self._operations.pop(operation.operation_id, None)
                if current and not current.future.done():
                    current.future.cancel()
            return BridgeResult(
                False,
                "MINECRAFT_TIMEOUT",
                "Minecraft no procesó la solicitud dentro del tiempo esperado.",
            )

    async def poll(self, server_id: str) -> dict[str, Any] | None:
        now = time.time()
        async with self._lock:
            self.last_heartbeat_at = now
            self._cleanup_locked(now)
            candidates = sorted(
                self._operations.values(),
                key=lambda item: item.created_at,
            )
            for operation in candidates:
                if operation.future.done() or operation.leased_until > now:
                    continue
                operation.leased_until = now + self.config.lease_seconds
                operation.attempts += 1
                LOGGER.info(
                    "Operación entregada id=%s action=%s attempt=%s server_id=%s",
                    operation.operation_id,
                    operation.action,
                    operation.attempts,
                    server_id,
                )
                return operation.public_payload()
        return None

    async def complete(self, server_id: str, payload: dict[str, Any]) -> bool:
        operation_id = str(payload.get("operation_id", "")).strip()
        if not operation_id:
            return False

        result = BridgeResult(
            success=payload.get("success") is True,
            code=str(payload.get("code", "UNKNOWN"))[:100],
            message=str(payload.get("message", "Sin mensaje."))[:1000],
            username=(
                str(payload["username"])[:16]
                if payload.get("username") is not None
                else None
            ),
            old_username=(
                str(payload["old_username"])[:16]
                if payload.get("old_username") is not None
                else None
            ),
        )

        async with self._lock:
            self.last_heartbeat_at = time.time()
            operation = self._operations.pop(operation_id, None)
            if operation is None:
                return False
            if not operation.future.done():
                operation.future.set_result(result)

        LOGGER.info(
            "Operación completada id=%s code=%s success=%s server_id=%s",
            operation_id,
            result.code,
            result.success,
            server_id,
        )
        return True

    async def status(self) -> dict[str, Any]:
        now = time.time()
        async with self._lock:
            self._cleanup_locked(now)
            heartbeat_age = (
                None
                if self.last_heartbeat_at is None
                else max(0.0, now - self.last_heartbeat_at)
            )
            online = (
                heartbeat_age is not None
                and heartbeat_age <= self.config.heartbeat_timeout_seconds
            )
            return {
                "minecraftOnline": online,
                "heartbeatAgeSeconds": (
                    round(heartbeat_age, 1) if heartbeat_age is not None else None
                ),
                "pendingOperations": len(self._operations),
            }

    def _cleanup_locked(self, now: float) -> None:
        expired = [
            operation_id
            for operation_id, operation in self._operations.items()
            if operation.expires_at <= now
        ]
        for operation_id in expired:
            operation = self._operations.pop(operation_id)
            if not operation.future.done():
                operation.future.set_result(
                    BridgeResult(
                        False,
                        "OPERATION_EXPIRED",
                        "La solicitud expiró antes de llegar a Minecraft.",
                    )
                )


def build_panel_embed(bot_user: discord.ClientUser | None) -> discord.Embed:
    embed = discord.Embed(
        title=PANEL_TITLE,
        description=(
            "Vincula tu cuenta de Minecraft con Discord usando las opciones "
            "que aparecen debajo.\n\n"
            "Cada miembro puede mantener **un solo nickname registrado**. "
            "Puedes cambiarlo o eliminar la vinculación cuando lo necesites.\n\n"
            "**¿Necesitas ayuda?** Abre un ticket de soporte."
        ),
        colour=discord.Colour.from_rgb(255, 193, 7),
    )
    if bot_user:
        embed.set_footer(
            text=bot_user.display_name,
            icon_url=bot_user.display_avatar.url,
        )
    else:
        embed.set_footer(text="MystrixBot")
    return embed


def is_configured_guild(
    interaction: discord.Interaction,
    config: Config,
) -> bool:
    return (
        interaction.guild_id == config.guild_id
        and isinstance(interaction.user, discord.Member)
    )


def is_admin(interaction: discord.Interaction, config: Config) -> bool:
    member = interaction.user
    return isinstance(member, discord.Member) and (
        member.guild_permissions.administrator
        or any(role.id == config.admin_role_id for role in member.roles)
    )


def format_result(result: BridgeResult) -> str:
    icon = "✅" if result.success else "❌"
    return f"{icon} {result.message}"


async def safe_error(interaction: discord.Interaction, text: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True)
    else:
        await interaction.response.send_message(text, ephemeral=True)


class UsernameModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        title: str,
        action: str,
        broker: OperationBroker,
        config: Config,
    ) -> None:
        super().__init__(title=title, timeout=300)
        self.action = action
        self.broker = broker
        self.config = config
        self.username_input = discord.ui.TextInput(
            label="Nombre de Minecraft",
            placeholder="Ejemplo: GretoNow",
            min_length=3,
            max_length=16,
            required=True,
        )
        self.add_item(self.username_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not is_configured_guild(interaction, self.config):
            await interaction.response.send_message(
                "❌ Este formulario solo funciona dentro del servidor configurado.",
                ephemeral=True,
            )
            return

        username = str(self.username_input.value).strip()
        if not USERNAME_PATTERN.fullmatch(username):
            await interaction.response.send_message(
                "❌ El nickname debe tener entre 3 y 16 caracteres y usar "
                "solo letras, números o guion bajo.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await self.broker.submit(
            self.action,
            interaction.user.id,
            username,
        )
        await interaction.edit_original_response(content=format_result(result))

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
    ) -> None:
        LOGGER.exception("Error en modal", exc_info=error)
        await safe_error(interaction, "❌ Ocurrió un error interno.")


class UnlinkConfirmView(discord.ui.View):
    def __init__(
        self,
        *,
        owner_id: int,
        broker: OperationBroker,
        config: Config,
    ) -> None:
        super().__init__(timeout=120)
        self.owner_id = owner_id
        self.broker = broker
        self.config = config

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "❌ Esta confirmación pertenece a otro usuario.",
                ephemeral=True,
            )
            return False
        return is_configured_guild(interaction, self.config)

    def disable_buttons(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    @discord.ui.button(
        label="Confirmar desvinculación",
        style=discord.ButtonStyle.danger,
        custom_id="mystrix:unlink:confirm:v2",
    )
    async def confirm(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await self.broker.submit("UNLINK", interaction.user.id)
        self.disable_buttons()
        await interaction.edit_original_response(
            content=format_result(result),
            view=self,
        )
        self.stop()

    @discord.ui.button(
        label="Cancelar",
        style=discord.ButtonStyle.secondary,
        custom_id="mystrix:unlink:cancel:v2",
    )
    async def cancel(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        self.disable_buttons()
        await interaction.response.edit_message(
            content="La desvinculación fue cancelada.",
            view=self,
        )
        self.stop()


class WhitelistPanelView(discord.ui.View):
    def __init__(self, *, broker: OperationBroker, config: Config) -> None:
        super().__init__(timeout=None)
        self.broker = broker
        self.config = config

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not is_configured_guild(interaction, self.config):
            await interaction.response.send_message(
                "❌ Este panel solo funciona dentro del servidor configurado.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Agregar",
        emoji="✏️",
        style=discord.ButtonStyle.success,
        custom_id="mystrix:whitelist:add:v2",
    )
    async def add(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        await interaction.response.send_modal(
            UsernameModal(
                title="Registrar nickname",
                action="ADD",
                broker=self.broker,
                config=self.config,
            )
        )

    @discord.ui.button(
        label="Editar",
        emoji="🪪",
        style=discord.ButtonStyle.primary,
        custom_id="mystrix:whitelist:edit:v2",
    )
    async def edit(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        await interaction.response.send_modal(
            UsernameModal(
                title="Actualizar nickname",
                action="EDIT",
                broker=self.broker,
                config=self.config,
            )
        )

    @discord.ui.button(
        label="Desvincular",
        emoji="🗑️",
        style=discord.ButtonStyle.danger,
        custom_id="mystrix:whitelist:unlink:v2",
    )
    async def unlink(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        await interaction.response.send_message(
            "¿Seguro que deseas desvincular tu nickname de Minecraft?",
            view=UnlinkConfirmView(
                owner_id=interaction.user.id,
                broker=self.broker,
                config=self.config,
            ),
            ephemeral=True,
        )

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: discord.ui.Item[Any],
    ) -> None:
        LOGGER.exception("Error en panel", exc_info=error)
        await safe_error(interaction, "❌ Ocurrió un error interno.")


class MystrixBot(commands.Bot):
    def __init__(self, config: Config, broker: OperationBroker) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(
            command_prefix=commands.when_mentioned,
            intents=intents,
            application_id=config.application_id,
        )
        self.config = config
        self.broker = broker
        self.guild_object = discord.Object(id=config.guild_id)

    async def setup_hook(self) -> None:
        self.add_view(
            WhitelistPanelView(broker=self.broker, config=self.config)
        )
        register_commands(self)
        await self.tree.sync(guild=self.guild_object)
        LOGGER.info("Comandos sincronizados en guild_id=%s", self.config.guild_id)

    async def on_ready(self) -> None:
        if self.user:
            LOGGER.info("Bot conectado como %s (%s)", self.user, self.user.id)


def register_commands(bot: MystrixBot) -> None:
    guild = bot.guild_object

    @bot.tree.command(
        name="whitelist-panel",
        description="Publica o actualiza el panel de whitelist.",
        guild=guild,
    )
    async def whitelist_panel(interaction: discord.Interaction) -> None:
        if not is_admin(interaction, bot.config):
            await interaction.response.send_message(
                "❌ No tienes permiso para publicar el panel.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        channel = bot.get_channel(bot.config.panel_channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(bot.config.panel_channel_id)
            except discord.DiscordException:
                await interaction.edit_original_response(
                    content="❌ No pude acceder al canal configurado."
                )
                return

        if not isinstance(channel, discord.TextChannel):
            await interaction.edit_original_response(
                content="❌ El canal configurado no es un canal de texto."
            )
            return

        view = WhitelistPanelView(broker=bot.broker, config=bot.config)
        existing: discord.Message | None = None
        try:
            async for message in channel.history(limit=100):
                if (
                    bot.user
                    and message.author.id == bot.user.id
                    and message.embeds
                    and message.embeds[0].title == PANEL_TITLE
                ):
                    existing = message
                    break
        except discord.DiscordException:
            existing = None

        if existing:
            await existing.edit(embed=build_panel_embed(bot.user), view=view)
            await interaction.edit_original_response(
                content="✅ El panel existente fue actualizado."
            )
        else:
            await channel.send(embed=build_panel_embed(bot.user), view=view)
            await interaction.edit_original_response(
                content=f"✅ Panel publicado en {channel.mention}."
            )

    @bot.tree.command(
        name="whitelist-health",
        description="Comprueba la conexión entre Render y Minecraft.",
        guild=guild,
    )
    async def whitelist_health(interaction: discord.Interaction) -> None:
        if not is_admin(interaction, bot.config):
            await interaction.response.send_message(
                "❌ No tienes permiso para comprobar el puente.",
                ephemeral=True,
            )
            return
        status = await bot.broker.status()
        icon = "✅" if status["minecraftOnline"] else "❌"
        age = status["heartbeatAgeSeconds"]
        await interaction.response.send_message(
            f"{icon} **Puente de Minecraft**\n"
            f"Estado: `{'conectado' if status['minecraftOnline'] else 'sin conexión'}`\n"
            f"Último sondeo: `{'nunca' if age is None else f'{age} s'}`\n"
            f"Solicitudes pendientes: `{status['pendingOperations']}`",
            ephemeral=True,
        )

    @bot.tree.command(
        name="whitelist-lookup",
        description="Consulta el nickname vinculado a un usuario.",
        guild=guild,
    )
    @app_commands.describe(usuario="Usuario de Discord que deseas consultar")
    async def whitelist_lookup(
        interaction: discord.Interaction,
        usuario: discord.Member,
    ) -> None:
        if not is_admin(interaction, bot.config):
            await interaction.response.send_message(
                "❌ No tienes permiso para consultar vínculos.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await bot.broker.submit("LOOKUP", usuario.id)
        if result.success and result.username:
            content = f"✅ {usuario.mention} tiene vinculado `{result.username}`."
        elif result.code == "NOT_LINKED":
            content = f"ℹ️ {usuario.mention} no tiene ningún nickname vinculado."
        else:
            content = format_result(result)
        await interaction.edit_original_response(content=content)

    @bot.tree.command(
        name="whitelist-remove",
        description="Desvincula administrativamente a un usuario.",
        guild=guild,
    )
    @app_commands.describe(usuario="Usuario que deseas desvincular")
    async def whitelist_remove(
        interaction: discord.Interaction,
        usuario: discord.Member,
    ) -> None:
        if not is_admin(interaction, bot.config):
            await interaction.response.send_message(
                "❌ No tienes permiso para eliminar vínculos.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await bot.broker.submit("UNLINK", usuario.id)
        await interaction.edit_original_response(content=format_result(result))


def authorized(request: web.Request, config: Config) -> bool:
    authorization = request.headers.get("Authorization", "")
    prefix = "Bearer "
    if not authorization.startswith(prefix):
        return False
    supplied = authorization[len(prefix):].strip()
    return bool(supplied) and hmac.compare_digest(supplied, config.bridge_secret)


def create_web_app(
    bot: MystrixBot,
    broker: OperationBroker,
    config: Config,
) -> web.Application:
    app = web.Application(client_max_size=64 * 1024)

    async def root(request: web.Request) -> web.Response:
        status = await broker.status()
        return web.json_response(
            {
                "service": "MystrixWhitelistRenderBridge",
                "version": BOT_VERSION,
                "discordReady": bot.is_ready(),
                **status,
            }
        )

    async def health(request: web.Request) -> web.Response:
        status = await broker.status()
        return web.json_response(
            {
                "success": True,
                "status": "healthy",
                "discordReady": bot.is_ready(),
                **status,
            }
        )

    async def ready(request: web.Request) -> web.Response:
        ready_state = bot.is_ready() and not bot.is_closed()
        return web.json_response(
            {"success": ready_state, "status": "ready" if ready_state else "starting"},
            status=200 if ready_state else 503,
        )

    async def poll(request: web.Request) -> web.Response:
        if not authorized(request, config):
            return web.json_response(
                {"success": False, "code": "UNAUTHORIZED"},
                status=401,
            )
        server_id = (
            request.query.get("server_id")
            or request.headers.get("X-Server-ID")
            or ""
        ).strip()
        if not hmac.compare_digest(server_id, config.bridge_server_id):
            return web.json_response(
                {"success": False, "code": "INVALID_SERVER_ID"},
                status=403,
            )
        operation = await broker.poll(server_id)
        return web.json_response({"success": True, "operation": operation})

    async def result(request: web.Request) -> web.Response:
        if not authorized(request, config):
            return web.json_response(
                {"success": False, "code": "UNAUTHORIZED"},
                status=401,
            )
        try:
            payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            return web.json_response(
                {"success": False, "code": "INVALID_JSON"},
                status=400,
            )
        if not isinstance(payload, dict):
            return web.json_response(
                {"success": False, "code": "INVALID_JSON"},
                status=400,
            )
        server_id = str(payload.get("server_id", "")).strip()
        if not hmac.compare_digest(server_id, config.bridge_server_id):
            return web.json_response(
                {"success": False, "code": "INVALID_SERVER_ID"},
                status=403,
            )
        accepted = await broker.complete(server_id, payload)
        return web.json_response(
            {
                "success": accepted,
                "code": "RESULT_ACCEPTED" if accepted else "UNKNOWN_OPERATION",
            },
            status=200 if accepted else 404,
        )

    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_get("/ready", ready)
    app.router.add_get("/api/v1/bridge/poll", poll)
    app.router.add_post("/api/v1/bridge/result", result)
    return app


async def run_application() -> None:
    configure_logging()
    config = Config.load()
    broker = OperationBroker(config)
    bot = MystrixBot(config, broker)

    app = create_web_app(bot, broker, config)
    runner = web.AppRunner(app, access_log=LOGGER)
    await runner.setup()
    site = web.TCPSite(runner, host=config.web_host, port=config.web_port)
    await site.start()
    LOGGER.info("Web service iniciado en %s:%s", config.web_host, config.web_port)

    try:
        await bot.start(config.discord_token, reconnect=True)
    finally:
        if not bot.is_closed():
            await bot.close()
        await runner.cleanup()


def main() -> None:
    try:
        asyncio.run(run_application())
    except KeyboardInterrupt:
        LOGGER.info("Cierre solicitado.")
    except RuntimeError as exc:
        configure_logging()
        LOGGER.critical("Configuración inválida: %s", exc)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
