"""Application factory and entry point.

    uvicorn wcd_api.main:app            # settings from the environment / .env
    wcd-api --host 0.0.0.0 --port 8000  # the same, through the console script
"""
from __future__ import annotations

import argparse
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .db import Database
from .queries import BadRequest, NotFound, Unavailable
from .queries.article import resolve_title
from .routers import v1, v2
from .settings import Settings

log = logging.getLogger("wcd_api")

#: apps/frontend — the dashboard, served at / (and /static) when present
FRONTEND_DIR = Path(__file__).resolve().parents[3] / "frontend"


def create_app(settings: Settings | None = None, *, title_resolver=resolve_title, frontend_dir: Path | None = FRONTEND_DIR) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.db = Database(settings)
        log.info("wcd_api: Postgres %s, Parquet %s", settings.database_url.split("@")[-1], settings.parquet_dir)
        yield
        app.state.db.dispose()

    app = FastAPI(title="Wikipedia Citations Database API", version="2.0.0a0", lifespan=lifespan,
                  description="Page-centric citation analytics over the WCD data model; see docs/design/proposed_api.md.")
    app.state.settings = settings
    app.state.title_resolver = title_resolver
    if settings.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["GET"], allow_headers=["*"])

    @app.exception_handler(NotFound)
    async def _not_found(_: Request, exc: NotFound):
        return JSONResponse(status_code=404, content={"error": str(exc), "code": 404})

    @app.exception_handler(BadRequest)
    async def _bad_request(_: Request, exc: BadRequest):
        return JSONResponse(status_code=400, content={"error": str(exc), "code": 400})

    @app.exception_handler(Unavailable)
    async def _unavailable(_: Request, exc: Unavailable):
        return JSONResponse(status_code=503, content={"error": str(exc), "code": 503})

    @app.get("/healthz", tags=["meta"])
    def healthz():
        return {"status": "ok"}

    app.include_router(v2.router)
    app.include_router(v1.router)

    if frontend_dir is not None and frontend_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

        @app.get("/", include_in_schema=False)
        def index():
            return FileResponse(frontend_dir / "index.html")

    return app


def __getattr__(name):
    # `uvicorn wcd_api.main:app` builds the app from the environment on first access
    if name == "app":
        return create_app()
    raise AttributeError(name)


def serve(argv=None) -> int:
    import uvicorn

    ap = argparse.ArgumentParser(description="Serve the Wikipedia Citations Database API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--log-level", default="info")
    args = ap.parse_args(argv)
    uvicorn.run("wcd_api.main:app", host=args.host, port=args.port, workers=args.workers, log_level=args.log_level, factory=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(serve())
