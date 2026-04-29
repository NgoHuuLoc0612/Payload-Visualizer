"""
FormatRegistry — discovers, registers, and dispatches all payload format processors.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from processors.base import BaseProcessor

log = structlog.get_logger(__name__)

_PROCESSOR_PKG = "processors"


class FormatRegistry:
    """Thread-safe registry for all payload format processors."""

    def __init__(self) -> None:
        self._processors: dict[str, "BaseProcessor"] = {}

    async def initialize(self) -> None:
        import processors  # noqa: F401 — trigger __init__

        pkg = importlib.import_module(_PROCESSOR_PKG)
        for _finder, name, _ispkg in pkgutil.iter_modules(pkg.__path__):
            if name == "base":
                continue
            try:
                mod = importlib.import_module(f"{_PROCESSOR_PKG}.{name}")
                for attr in dir(mod):
                    obj = getattr(mod, attr)
                    if (
                        isinstance(obj, type)
                        and hasattr(obj, "FORMAT_ID")
                        and obj.__name__ != "BaseProcessor"
                    ):
                        instance = obj()
                        self._processors[instance.FORMAT_ID] = instance
                        log.info("registered", fmt=instance.FORMAT_ID, processor=obj.__name__)
            except Exception as exc:
                log.warning("processor_load_failed", module=name, error=str(exc))

        log.info("registry_ready", count=len(self._processors))

    def get(self, fmt: str) -> "BaseProcessor | None":
        return self._processors.get(fmt)

    def all(self) -> dict[str, "BaseProcessor"]:
        return dict(self._processors)

    def format_ids(self) -> list[str]:
        return sorted(self._processors.keys())

    def detect(self, raw: bytes) -> "BaseProcessor | None":
        """Heuristically detect format from raw bytes."""
        for proc in self._processors.values():
            if proc.can_detect(raw):
                return proc
        return None
