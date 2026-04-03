# api/main.py
from dotenv import load_dotenv
load_dotenv()

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import get_settings
from dependencies import init_pool, close_pool
from routers.ops import router as ops_router


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _allowed_origins(environment: str) -> list[str]:
    if environment == "production":
        return ["https://your-ops-dashboard.example.com"]
    if environment == "staging":
        return ["https://staging-dashboard.example.com"]
    return ["http://localhost:3000", "http://localhost:8080"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)

    logging.getLogger(__name__).info(
        "Starting API",
        extra={"environment": settings.environment}
    )

    await init_pool()
    yield
    await close_pool()

    logging.getLogger(__name__).info("API shutdown complete")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Eagle Eyes Backend",
        version="1.0.0",
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url=None,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins(settings.environment),
        allow_methods=["GET"],
        allow_headers=["Authorization", "Content-Type"],
    )

    app.include_router(ops_router)

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        """Kubernetes liveness probe."""
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz():
        """Kubernetes readiness probe — verifies DB pool is live."""
        from dependencies import get_pool
        pool = await get_pool()
        await pool.fetchval("SELECT 1")
        return {"status": "ready"}

    return app


app = create_app()