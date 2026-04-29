"""
REST API routes — parse, detect, diff, transform, validate, export, stream-history.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Annotated, Any

import orjson
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from core.config import Settings
from processors.base import ParseResult

router = APIRouter(tags=["Payload"])
settings = Settings()


# ── Request / Response models ─────────────────────────────────────────────────

class ParseRequest(BaseModel):
    payload: str                      # raw text or base64
    format: str = "auto"
    encoding: str = "utf-8"
    base64_input: bool = False


class DiffRequest(BaseModel):
    left: str
    right: str
    format: str = "json"


class TransformRequest(BaseModel):
    payload: str
    source_format: str
    target_format: str


class ValidateRequest(BaseModel):
    payload: str
    format: str
    schema: dict[str, Any] | None = None


class QueryRequest(BaseModel):
    payload: str
    format: str
    jq_expression: str


# ── Helpers ────────────────────────────────────────────────────────────────────

def _registry(req: Request):
    return req.app.state.registry


def _to_bytes(text: str, encoding: str = "utf-8", is_b64: bool = False) -> bytes:
    if is_b64:
        return base64.b64decode(text + "==")
    return text.encode(encoding, errors="replace")


def _result_response(result: ParseResult) -> JSONResponse:
    return JSONResponse(content=orjson.loads(result.model_dump_json_bytes()))


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/formats")
async def list_formats(request: Request):
    """Return all registered format processors."""
    registry = _registry(request)
    return {
        fmt_id: {
            "id": proc.FORMAT_ID,
            "label": proc.FORMAT_LABEL,
        }
        for fmt_id, proc in registry.all().items()
    }


@router.post("/parse")
async def parse_payload(body: ParseRequest, request: Request):
    """Parse a payload and return full analysis."""
    registry = _registry(request)
    raw = _to_bytes(body.payload, body.encoding, body.base64_input)

    if len(raw) > settings.max_payload_bytes:
        raise HTTPException(413, f"Payload too large: {len(raw)} bytes (max {settings.max_payload_bytes})")

    fmt = body.format
    if fmt == "auto":
        # Priority-ordered detection: specific formats first
        DETECT_ORDER = ["jwt", "json", "xml", "yaml", "toml", "har", "csv", "graphql", "msgpack", "protobuf", "binary"]
        proc = None
        for fid in DETECT_ORDER:
            p = registry.get(fid)
            if p and p.can_detect(raw):
                proc = p
                break
        # Fallback: try parsing each until one succeeds
        if proc is None:
            for fid in ["json", "xml", "yaml", "toml", "csv", "graphql"]:
                p = registry.get(fid)
                try:
                    if p:
                        p.parse(raw)
                        proc = p
                        break
                except Exception:
                    continue
        if proc is None:
            raise HTTPException(422, "Could not auto-detect format. Please select manually.")
    else:
        proc = registry.get(fmt)
        if proc is None:
            raise HTTPException(404, f"Unknown format '{fmt}'")

    try:
        result = proc.parse(raw)
    except Exception as exc:
        raise HTTPException(422, f"Parse failed: {exc}") from exc

    return _result_response(result)


@router.post("/parse/file")
async def parse_file(
    request: Request,
    file: UploadFile = File(...),
    format: str = Query(default="auto"),
):
    """Parse an uploaded file."""
    registry = _registry(request)
    raw = await file.read()
    if len(raw) > settings.max_payload_bytes:
        raise HTTPException(413, "File too large")

    if format == "auto":
        proc = registry.detect(raw)
        if proc is None:
            # Use filename extension as hint
            ext = (file.filename or "").rsplit(".", 1)[-1].lower()
            proc = registry.get(ext)
        if proc is None:
            raise HTTPException(422, "Could not detect format")
    else:
        proc = registry.get(format)
        if proc is None:
            raise HTTPException(404, f"Unknown format: {format}")

    try:
        result = proc.parse(raw)
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc

    return _result_response(result)


@router.post("/detect")
async def detect_format(body: ParseRequest, request: Request):
    """Auto-detect the format of a payload without full parsing."""
    registry = _registry(request)
    raw = _to_bytes(body.payload, body.encoding, body.base64_input)
    proc = registry.detect(raw)
    if proc is None:
        return {"detected": None, "confidence": 0.0}
    return {"detected": proc.FORMAT_ID, "label": proc.FORMAT_LABEL, "confidence": 0.9}


@router.post("/diff")
async def diff_payloads(body: DiffRequest, request: Request):
    """Compute a structural diff between two payloads of the same format."""
    registry = _registry(request)
    proc = registry.get(body.format)
    if proc is None:
        raise HTTPException(404, f"Unknown format: {body.format}")

    left_raw = body.left.encode()
    right_raw = body.right.encode()

    try:
        left_result = proc.parse(left_raw)
        right_result = proc.parse(right_raw)
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc

    diff = _compute_diff(left_result.parsed, right_result.parsed)
    return {
        "left_sha256": left_result.sha256,
        "right_sha256": right_result.sha256,
        "changes": diff,
        "change_count": len(diff),
        "left_stats": left_result.stats.model_dump(),
        "right_stats": right_result.stats.model_dump(),
    }


def _compute_diff(left: Any, right: Any, path: str = "") -> list[dict]:
    changes = []
    if type(left) != type(right):
        changes.append({"path": path or "root", "op": "type_change", "from": type(left).__name__, "to": type(right).__name__})
        return changes
    if isinstance(left, dict):
        all_keys = set(left) | set(right)
        for k in all_keys:
            p = f"{path}.{k}" if path else k
            if k not in left:
                changes.append({"path": p, "op": "added", "value": right[k]})
            elif k not in right:
                changes.append({"path": p, "op": "removed", "value": left[k]})
            else:
                changes.extend(_compute_diff(left[k], right[k], p))
    elif isinstance(left, list):
        max_len = max(len(left), len(right))
        for i in range(max_len):
            p = f"{path}[{i}]"
            if i >= len(left):
                changes.append({"path": p, "op": "added", "value": right[i]})
            elif i >= len(right):
                changes.append({"path": p, "op": "removed", "value": left[i]})
            else:
                changes.extend(_compute_diff(left[i], right[i], p))
    else:
        if left != right:
            changes.append({"path": path or "root", "op": "modified", "from": left, "to": right})
    return changes


@router.post("/transform")
async def transform_payload(body: TransformRequest, request: Request):
    """Transform a payload from one format to another (where supported)."""
    registry = _registry(request)
    proc_src = registry.get(body.source_format)
    if proc_src is None:
        raise HTTPException(404, f"Unknown source format: {body.source_format}")

    raw = body.payload.encode()
    try:
        result = proc_src.parse(raw)
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc

    target = body.target_format.lower()

    if target == "json":
        out = json.dumps(result.parsed, indent=2, default=str)
        media = "application/json"
    elif target == "yaml":
        try:
            import yaml
            out = yaml.dump(result.parsed, default_flow_style=False, allow_unicode=True)
        except ImportError:
            out = json.dumps(result.parsed, indent=2, default=str)
        media = "application/yaml"
    elif target == "toml":
        try:
            import tomli_w
            out = tomli_w.dumps(result.parsed if isinstance(result.parsed, dict) else {"data": result.parsed})
        except Exception:
            out = json.dumps(result.parsed, indent=2, default=str)
        media = "application/toml"
    elif target == "msgpack":
        try:
            import msgpack
            out_bytes = msgpack.packb(result.parsed, use_bin_type=True)
            return Response(content=out_bytes, media_type="application/x-msgpack")
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc
    elif target == "csv":
        if isinstance(result.parsed, list) and result.parsed:
            import csv, io
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=list(result.parsed[0].keys()))
            writer.writeheader()
            writer.writerows(result.parsed)
            out = buf.getvalue()
            media = "text/csv"
        else:
            raise HTTPException(422, "Source must be a JSON array to convert to CSV")
    else:
        raise HTTPException(400, f"Unsupported target format: {target}")

    return Response(content=out, media_type=media)


@router.post("/validate")
async def validate_payload(body: ValidateRequest, request: Request):
    """Validate payload against JSON Schema (when provided)."""
    registry = _registry(request)
    proc = registry.get(body.format)
    if proc is None:
        raise HTTPException(404, f"Unknown format: {body.format}")

    raw = body.payload.encode()
    try:
        result = proc.parse(raw)
    except Exception as exc:
        return {"valid": False, "errors": [str(exc)], "anomalies": []}

    errors = []
    if body.schema:
        try:
            import jsonschema
            jsonschema.validate(result.parsed, body.schema)
        except jsonschema.ValidationError as ve:
            errors.append(str(ve.message))
        except Exception as exc:
            errors.append(str(exc))

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "anomalies": [a.model_dump() for a in result.anomalies],
        "stats": result.stats.model_dump(),
    }


@router.post("/query")
async def query_payload(body: QueryRequest, request: Request):
    """Apply a jq-style expression to a JSON payload."""
    registry = _registry(request)
    proc = registry.get(body.format)
    if proc is None:
        raise HTTPException(404, f"Unknown format: {body.format}")

    raw = body.payload.encode()
    try:
        result = proc.parse(raw)
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc

    try:
        import pyjq  # type: ignore
        out = pyjq.first(body.jq_expression, result.parsed)
    except ImportError:
        # Fallback: simple dot-notation path
        out = _simple_path(result.parsed, body.jq_expression)
    except Exception as exc:
        raise HTTPException(422, f"Query error: {exc}") from exc

    return {"result": out, "expression": body.jq_expression}


def _simple_path(obj: Any, expr: str) -> Any:
    """Minimal dot/bracket path resolver fallback."""
    parts = re.split(r"\.|(?=\[)", expr.lstrip("."))
    cur = obj
    for part in parts:
        if not part:
            continue
        if part.startswith("[") and part.endswith("]"):
            idx = int(part[1:-1])
            cur = cur[idx]
        else:
            cur = cur[part]
    return cur


import re


@router.get("/health")
async def health():
    return {"status": "ok", "ts": time.time()}
