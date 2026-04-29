"""
BaseProcessor — abstract contract for every payload format processor.
All processors emit a unified ParseResult consumed by the API and WebSocket layers.
"""

from __future__ import annotations

import hashlib
import time
from abc import ABC, abstractmethod
from typing import Any

import orjson
from pydantic import BaseModel, Field


# ── Result Model ───────────────────────────────────────────────────────────────

class FieldInfo(BaseModel):
    path: str
    type: str
    value: Any
    size_bytes: int = 0
    depth: int = 0
    nullable: bool = False
    repeated: bool = False


class SchemaNode(BaseModel):
    name: str
    type: str
    children: list["SchemaNode"] = Field(default_factory=list)
    nullable: bool = False
    repeated: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


SchemaNode.model_rebuild()


class Statistics(BaseModel):
    total_fields: int = 0
    max_depth: int = 0
    null_count: int = 0
    array_count: int = 0
    object_count: int = 0
    string_count: int = 0
    number_count: int = 0
    boolean_count: int = 0
    unique_keys: int = 0
    entropy: float = 0.0


class Anomaly(BaseModel):
    severity: str          # "info" | "warn" | "error"
    code: str
    message: str
    path: str = ""


class ParseResult(BaseModel):
    format_id: str
    format_label: str
    raw_size: int
    parse_time_ms: float
    sha256: str

    parsed: Any = None
    schema_tree: SchemaNode | None = None
    fields: list[FieldInfo] = Field(default_factory=list)
    stats: Statistics = Field(default_factory=Statistics)
    anomalies: list[Anomaly] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    pretty: str = ""

    def model_dump_json_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())


# ── Base Processor ─────────────────────────────────────────────────────────────

class BaseProcessor(ABC):
    FORMAT_ID: str = ""
    FORMAT_LABEL: str = ""

    @abstractmethod
    def parse(self, raw: bytes) -> ParseResult:
        ...

    def can_detect(self, raw: bytes) -> bool:  # noqa: ARG002
        return False

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _sha256(raw: bytes) -> str:
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _now_ms() -> float:
        return time.monotonic() * 1000

    def _base_result(self, raw: bytes, t0: float) -> ParseResult:
        return ParseResult(
            format_id=self.FORMAT_ID,
            format_label=self.FORMAT_LABEL,
            raw_size=len(raw),
            parse_time_ms=round(self._now_ms() - t0, 3),
            sha256=self._sha256(raw),
        )

    # ── generic tree builders ─────────────────────────────────────────────────

    @staticmethod
    def _build_schema(obj: Any, name: str = "root") -> SchemaNode:
        if isinstance(obj, dict):
            node = SchemaNode(name=name, type="object")
            for k, v in obj.items():
                node.children.append(BaseProcessor._build_schema(v, k))
            return node
        if isinstance(obj, list):
            node = SchemaNode(name=name, type="array", repeated=True)
            if obj:
                node.children.append(BaseProcessor._build_schema(obj[0], "item"))
            return node
        if obj is None:
            return SchemaNode(name=name, type="null", nullable=True)
        return SchemaNode(name=name, type=type(obj).__name__)

    @staticmethod
    def _collect_fields(obj: Any, path: str = "", depth: int = 0) -> list[FieldInfo]:
        fields: list[FieldInfo] = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                child_path = f"{path}.{k}" if path else k
                fields.append(FieldInfo(
                    path=child_path,
                    type=type(v).__name__,
                    value=v if not isinstance(v, (dict, list)) else None,
                    depth=depth,
                    nullable=v is None,
                    repeated=isinstance(v, list),
                    size_bytes=len(str(v).encode()),
                ))
                fields.extend(BaseProcessor._collect_fields(v, child_path, depth + 1))
        elif isinstance(obj, list):
            for i, item in enumerate(obj[:20]):  # cap at 20 items
                child_path = f"{path}[{i}]"
                fields.extend(BaseProcessor._collect_fields(item, child_path, depth + 1))
        return fields

    @staticmethod
    def _compute_stats(obj: Any, fields: list[FieldInfo]) -> Statistics:
        import math, collections
        stats = Statistics(total_fields=len(fields))
        stats.max_depth = max((f.depth for f in fields), default=0)
        stats.null_count = sum(1 for f in fields if f.nullable)
        stats.array_count = sum(1 for f in fields if f.repeated)
        stats.object_count = sum(1 for f in fields if f.type == "dict")
        stats.string_count = sum(1 for f in fields if f.type == "str")
        stats.number_count = sum(1 for f in fields if f.type in ("int", "float"))
        stats.boolean_count = sum(1 for f in fields if f.type == "bool")
        keys = [f.path.split(".")[-1] for f in fields]
        stats.unique_keys = len(set(keys))

        # Shannon entropy over value types
        counter = collections.Counter(f.type for f in fields)
        total = len(fields) or 1
        stats.entropy = round(
            -sum((c / total) * math.log2(c / total) for c in counter.values() if c),
            4,
        )
        return stats
