# scripts/run_deployments.py
# One-shot manual trigger for testing
# Run: python scripts/run_deployments.py

import asyncio
import logging
from dotenv import load_dotenv
load_dotenv()

import asyncpg
from config.settings import get_settings
from services.execute_deployments import execute_deployments
from services.alerting import AlertService

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")


async def main():
    settings = get_settings()
    pool     = await asyncpg.create_pool(dsn=settings.database_url,
                                          min_size=1, max_size=3)
    await execute_deployments(pool, AlertService(), max_concurrent=1)
    await pool.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())