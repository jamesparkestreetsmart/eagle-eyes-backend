# services/realtime_listener.py

import asyncio
import logging
from typing import Callable, Awaitable

from realtime import AsyncRealtimeClient

logger = logging.getLogger(__name__)


async def listen_for_deployments(
    supabase_url: str,
    supabase_anon_key: str,
    on_change: Callable[[], Awaitable[None]],
) -> None:
    """
    Connect to Supabase Realtime and listen for changes on
    c_ha_automation_deployments.

    Calls on_change() immediately when:
    - A new deployment row is inserted
    - An existing row is updated with push_confirmed = false

    Reconnects automatically on disconnect.
    """
    url = supabase_url.replace("https://", "wss://") + "/realtime/v1"

    while True:
        try:
            logger.info("Connecting to Supabase Realtime...")

            client = AsyncRealtimeClient(
                url,
                supabase_anon_key,
                auto_reconnect=True,
                params={"apikey": supabase_anon_key},
            )

            await client.connect()

            def handle_insert(payload):
                logger.info("Realtime: new deployment row — triggering sweep")
                asyncio.create_task(on_change())

            def handle_update(payload):
                record = payload.get("record", {})
                if not record.get("push_confirmed", True):
                    logger.info("Realtime: deployment reset to pending — triggering sweep")
                    asyncio.create_task(on_change())

            channel = client.channel("deployments")
            channel.on_postgres_changes(
                event="INSERT",
                schema="public",
                table="c_ha_automation_deployments",
                callback=handle_insert,
            )
            channel.on_postgres_changes(
                event="UPDATE",
                schema="public",
                table="c_ha_automation_deployments",
                callback=handle_update,
            )
            def on_subscribe(status, err):
                if err:
                    logger.error(f"Realtime subscribe error: {err}")
                else:
                    logger.info(f"Realtime subscribed — status: {status}")

            await channel.subscribe(on_subscribe)

            logger.info("Realtime listener active — watching c_ha_automation_deployments")

            # keep alive by sleeping in a loop
            # heartbeat is handled internally by the client
            while True:
                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info("Realtime listener cancelled")
            return
        except Exception as e:
            logger.error(f"Realtime connection error: {e} — reconnecting in 10s")
            await asyncio.sleep(10)