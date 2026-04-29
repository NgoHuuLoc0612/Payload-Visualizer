"""
Payload Visualizer — Enterprise-grade multi-format payload analysis platform.
Supports JSON, XML, CBOR, MsgPack, Protobuf, GraphQL, JWT, MQTT, AMQP, gRPC.
"""

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from api.routes import router as api_router
from api.websocket import router as ws_router
from core.registry import FormatRegistry
from core.config import Settings

# ── Bootstrap ─────────────────────────────────────────────────────────────────
console = Console()
settings = Settings()

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup → yield → shutdown."""
    registry = FormatRegistry()
    await registry.initialize()
    app.state.registry = registry

    console.print(Panel(
        Text.assemble(
            ("  Payload Visualizer  ", "bold white on #0a0a0a"),
            "\n\n",
            ("  Host  ", "dim"), (f"http://{settings.host}:{settings.port}\n", "cyan bold"),
            ("  Docs  ", "dim"), (f"http://{settings.host}:{settings.port}/docs\n", "cyan bold"),
            ("  WS    ", "dim"), (f"ws://{settings.host}:{settings.port}/ws/stream\n", "cyan bold"),
        ),
        border_style="#39ff14",
        title="[bold #39ff14]◈ READY[/]",
        padding=(1, 4),
    ))
    yield
    log.info("shutdown", msg="Payload Visualizer stopped")


# ── FastAPI App ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Payload Visualizer",
    description="Enterprise-grade multi-format payload analysis & visualization platform",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ─────────────────────────────────────────────────────────────────────
app.include_router(api_router, prefix="/api/v1")
app.include_router(ws_router)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse("static/index.html")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Serve a minimal inline SVG favicon to suppress 404 errors."""
    import base64
    # Tiny 16x16 green pixel favicon (ICO format, 1bpp)
    svg = b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><rect width="16" height="16" rx="3" fill="#0d1117"/><text x="2" y="13" font-size="12" fill="#39ff14">&#9670;</text></svg>'
    from fastapi.responses import Response
    return Response(content=svg, media_type="image/svg+xml")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info",
        access_log=False,
    )
