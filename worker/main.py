# worker/main.py

from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import signal

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.settings import get_settings
from services.alerting import AlertService
from services.realtime_listener import listen_for_deployments
from worker.scheduler import run_deployment_sweep


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting deployment worker", extra={
        "environment":    settings.environment,
        "sweep_interval": settings.retry_sweep_interval_seconds,
    })

    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
    )

    alert_service = AlertService()

    # shared sweep function — used by both scheduler and realtime trigger
    async def sweep():
        await run_deployment_sweep(
            pool=pool,
            alert_service=alert_service,
        )

    # debounce — prevent rapid-fire sweeps if multiple rows change at once
    _sweep_scheduled = False

    async def debounced_sweep():
        nonlocal _sweep_scheduled
        if _sweep_scheduled:
            return
        _sweep_scheduled = True
        await asyncio.sleep(2)   # wait 2s to batch rapid changes
        _sweep_scheduled = False
        await sweep()

    scheduler = AsyncIOScheduler()

    # interval sweep — fallback, runs every N seconds regardless
    scheduler.add_job(
        sweep,
        trigger="interval",
        seconds=settings.retry_sweep_interval_seconds,
        max_instances=1,
        misfire_grace_time=30,
        id="deployment_sweep_interval",
    )

    scheduler.start()
    logger.info(
        f"Scheduler started — sweep every "
        f"{settings.retry_sweep_interval_seconds}s"
    )

    # run one sweep immediately on startup
    logger.info("Running initial deployment sweep on startup")
    await sweep()

    # realtime listener — event trigger, fires immediately on DB change
    realtime_task = None
    if settings.supabase_url and settings.supabase_anon_key:
        logger.info("Starting Supabase Realtime listener")
        realtime_task = asyncio.create_task(
            listen_for_deployments(
                supabase_url=settings.supabase_url,
                supabase_anon_key=settings.supabase_anon_key,
                on_change=debounced_sweep,
            )
        )
    else:
        logger.warning(
            "SUPABASE_URL or SUPABASE_ANON_KEY not set — "
            "realtime trigger disabled, interval sweep only"
        )

    # clean shutdown on SIGTERM / SIGINT
    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set_result, None)
        except NotImplementedError:
            pass

    await stop

    logger.info("Shutdown signal received — draining scheduler")
    if realtime_task:
        realtime_task.cancel()
    scheduler.shutdown(wait=True)
    await pool.close()
    logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())