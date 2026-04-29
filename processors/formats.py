"""
All payload format processors consolidated into one module for clean project structure.
Formats: JSON · XML · MsgPack · JWT · GraphQL · CSV · Protobuf · YAML · TOML · Binary · HAR
"""

from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import json
import re
import struct
import time
import xml.etree.ElementTree as ET
from typing import Any

import orjson

from .base import (
    Anomaly, BaseProcessor, FieldInfo, ParseResult, SchemaNode, Statistics
)

# ── helpers ────────────────────────────────────────────────────────────────────

def _now_ms() -> float:
    return time.monotonic() * 1000


# ══════════════════════════════════════════════════════════════════════════════
# JSON
# ══════════════════════════════════════════════════════════════════════════════
class JsonProcessor(BaseProcessor):
    FORMAT_ID = "json"
    FORMAT_LABEL = "JSON"

    def can_detect(self, raw: bytes) -> bool:
        s = raw.lstrip()
        if s[:1] not in (b"{", b"["):
            return False
        # Must end with matching bracket to be valid JSON candidate
        e = raw.rstrip()
        return (s[:1] == b"{" and e[-1:] == b"}") or (s[:1] == b"[" and e[-1:] == b"]")

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        anomalies: list[Anomaly] = []

        try:
            obj = orjson.loads(raw)
        except Exception as exc:
            # fallback: try standard json (handles comments via strip)
            try:
                cleaned = re.sub(r"//[^\n]*|/\*.*?\*/", "", raw.decode(errors="replace"), flags=re.S)
                obj = json.loads(cleaned)
                anomalies.append(Anomaly(severity="warn", code="JSON_COMMENTS", message="JSON contained comments — stripped before parse"))
            except Exception:
                raise ValueError(f"JSON parse error: {exc}") from exc

        # deep analysis
        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)
        schema = self._build_schema(obj)
        anomalies += self._detect_anomalies(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = schema
        result.fields = fields[:500]
        result.stats = stats
        result.anomalies = anomalies
        result.pretty = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
        result.metadata = {
            "encoding": "utf-8",
            "top_level_type": type(obj).__name__,
            "array_length": len(obj) if isinstance(obj, list) else None,
        }
        return result

    def _detect_anomalies(self, obj: Any, fields: list[FieldInfo]) -> list[Anomaly]:
        issues: list[Anomaly] = []
        if any(f.nullable for f in fields):
            issues.append(Anomaly(severity="info", code="NULL_VALUES", message=f"{sum(1 for f in fields if f.nullable)} null value(s) detected"))
        depth = max((f.depth for f in fields), default=0)
        if depth > 8:
            issues.append(Anomaly(severity="warn", code="DEEP_NESTING", message=f"Max nesting depth {depth} exceeds recommended limit of 8"))
        dups = _find_duplicate_keys_json(obj)
        for dup in dups:
            issues.append(Anomaly(severity="warn", code="DUPLICATE_KEY", message=f"Duplicate key detected: '{dup}'", path=dup))
        return issues


def _find_duplicate_keys_json(obj: Any, seen: set | None = None) -> list[str]:
    dups = []
    if isinstance(obj, dict):
        keys = list(obj.keys())
        unique = set(keys)
        if len(keys) != len(unique):
            for k in unique:
                if keys.count(k) > 1:
                    dups.append(k)
        for v in obj.values():
            dups.extend(_find_duplicate_keys_json(v))
    elif isinstance(obj, list):
        for item in obj:
            dups.extend(_find_duplicate_keys_json(item))
    return dups


# ══════════════════════════════════════════════════════════════════════════════
# XML
# ══════════════════════════════════════════════════════════════════════════════
class XmlProcessor(BaseProcessor):
    FORMAT_ID = "xml"
    FORMAT_LABEL = "XML / SOAP"

    def can_detect(self, raw: bytes) -> bool:
        s = raw.lstrip()
        if not (s.startswith(b"<?xml") or s.startswith(b"<")):
            return False
        # Must have a closing tag or self-closing
        return b">" in s and (b"</" in s or b"/>" in s or s.startswith(b"<?xml"))

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        anomalies: list[Anomaly] = []

        try:
            root = ET.fromstring(raw.decode(errors="replace"))
        except ET.ParseError as exc:
            raise ValueError(f"XML parse error: {exc}") from exc

        obj = _xml_to_dict(root)
        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)
        schema = _xml_schema(root)

        namespaces = list({m for _, m in ET.iterparse(io.BytesIO(raw), events=["start-ns"])})
        if namespaces:
            anomalies.append(Anomaly(severity="info", code="XML_NAMESPACES", message=f"{len(namespaces)} namespace(s) found"))

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = schema
        result.fields = fields[:500]
        result.stats = stats
        result.anomalies = anomalies
        result.pretty = _pretty_xml(raw)
        result.metadata = {
            "root_tag": root.tag,
            "namespaces": [f"{p}={u}" for p, u in namespaces],
            "element_count": sum(1 for _ in root.iter()),
        }
        return result


def _xml_to_dict(el: ET.Element) -> dict:
    d: dict[str, Any] = {}
    if el.attrib:
        d["@attributes"] = dict(el.attrib)
    if el.text and el.text.strip():
        d["#text"] = el.text.strip()
    for child in el:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        child_d = _xml_to_dict(child)
        if tag in d:
            if not isinstance(d[tag], list):
                d[tag] = [d[tag]]
            d[tag].append(child_d)
        else:
            d[tag] = child_d
    return d


def _xml_schema(el: ET.Element, name: str = "root") -> SchemaNode:
    tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
    node = SchemaNode(name=name or tag, type="element")
    if el.attrib:
        node.children.append(SchemaNode(name="@attributes", type="attributes"))
    for child in el:
        child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        node.children.append(_xml_schema(child, child_tag))
    return node


def _pretty_xml(raw: bytes) -> str:
    try:
        import xml.dom.minidom
        return xml.dom.minidom.parseString(raw).toprettyxml(indent="  ")
    except Exception:
        return raw.decode(errors="replace")


# ══════════════════════════════════════════════════════════════════════════════
# MsgPack
# ══════════════════════════════════════════════════════════════════════════════
class MsgPackProcessor(BaseProcessor):
    FORMAT_ID = "msgpack"
    FORMAT_LABEL = "MessagePack"

    def can_detect(self, raw: bytes) -> bool:
        if len(raw) < 2:
            return False
        # Reject plain printable ASCII/UTF-8 text — that's never msgpack
        printable = sum(1 for b in raw[:32] if 0x20 <= b < 0x7f or b in (0x09, 0x0a, 0x0d))
        if printable / min(len(raw), 32) > 0.7:
            return False
        b = raw[0]
        return b in (0xc0, 0xc2, 0xc3, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf,
                     0xd0, 0xd1, 0xd2, 0xd3, 0xdc, 0xdd, 0xde, 0xdf) or (b & 0xe0) == 0xa0

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        try:
            import msgpack  # type: ignore
            obj = msgpack.unpackb(raw, raw=False, strict_map_key=False)
        except Exception as exc:
            raise ValueError(f"MsgPack decode error: {exc}") from exc

        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)
        schema = self._build_schema(obj)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = schema
        result.fields = fields[:500]
        result.stats = stats
        result.pretty = json.dumps(obj, indent=2, default=str)
        result.metadata = {
            "compression_ratio": round(len(json.dumps(obj, default=str).encode()) / max(len(raw), 1), 2),
            "binary_size": len(raw),
        }
        return result


# ══════════════════════════════════════════════════════════════════════════════
# JWT
# ══════════════════════════════════════════════════════════════════════════════
class JwtProcessor(BaseProcessor):
    FORMAT_ID = "jwt"
    FORMAT_LABEL = "JWT / JWS"

    def can_detect(self, raw: bytes) -> bool:
        parts = raw.strip().split(b".")
        if len(parts) != 3:
            return False
        # Each part must be non-empty base64url and at least the header must decode to JSON with alg
        if not all(len(p) > 2 and re.match(rb"^[A-Za-z0-9_\-]+$", p) for p in parts):
            return False
        try:
            import base64, json
            h = parts[0] + b"==" 
            header = json.loads(base64.urlsafe_b64decode(h))
            return "alg" in header or "typ" in header
        except Exception:
            return False

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        anomalies: list[Anomaly] = []
        parts = raw.strip().split(b".")
        if len(parts) != 3:
            raise ValueError("JWT must have exactly 3 parts separated by '.'")

        header_raw, payload_raw, sig_raw = parts
        header = _b64_decode_json(header_raw)
        payload = _b64_decode_json(payload_raw)

        # Anomaly checks
        alg = header.get("alg", "")
        if alg == "none":
            anomalies.append(Anomaly(severity="error", code="JWT_ALG_NONE", message="Algorithm 'none' is insecure — signature not verified"))
        if alg in ("HS256", "HS384", "HS512"):
            anomalies.append(Anomaly(severity="warn", code="JWT_HMAC", message=f"HMAC algorithm {alg}: shared-secret risk if exposed"))

        import time as _time
        now = int(_time.time())
        if "exp" in payload and payload["exp"] < now:
            anomalies.append(Anomaly(severity="error", code="JWT_EXPIRED", message=f"Token expired at {payload['exp']}"))
        if "nbf" in payload and payload["nbf"] > now:
            anomalies.append(Anomaly(severity="warn", code="JWT_NOT_YET_VALID", message="Token not yet valid (nbf in future)"))
        if "iat" not in payload:
            anomalies.append(Anomaly(severity="info", code="JWT_NO_IAT", message="No 'iat' (issued-at) claim"))

        obj = {"header": header, "payload": payload, "signature": sig_raw.decode()}
        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = self._build_schema(obj)
        result.fields = fields
        result.stats = stats
        result.anomalies = anomalies
        result.pretty = json.dumps(obj, indent=2, default=str)
        result.metadata = {
            "algorithm": alg,
            "type": header.get("typ", ""),
            "issuer": payload.get("iss"),
            "subject": payload.get("sub"),
            "audience": payload.get("aud"),
            "claims": list(payload.keys()),
        }
        return result


def _b64_decode_json(data: bytes) -> dict:
    padding = 4 - len(data) % 4
    padded = data + b"=" * (padding % 4)
    return json.loads(base64.urlsafe_b64decode(padded))


# ══════════════════════════════════════════════════════════════════════════════
# GraphQL
# ══════════════════════════════════════════════════════════════════════════════
class GraphQLProcessor(BaseProcessor):
    FORMAT_ID = "graphql"
    FORMAT_LABEL = "GraphQL"

    def can_detect(self, raw: bytes) -> bool:
        s = raw.lstrip().decode(errors="replace")
        # Must start with GQL keywords (not plain "{" which is JSON)
        return any(s.startswith(kw) for kw in ("query ", "query{", "mutation ", "mutation{", "subscription ", "fragment "))

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        text = raw.decode(errors="replace")
        anomalies: list[Anomaly] = []

        op_type = "unknown"
        for kw in ("subscription", "mutation", "query"):
            if re.search(rf"\b{kw}\b", text, re.I):
                op_type = kw
                break

        # Extract operation name
        op_match = re.search(r"\b(?:query|mutation|subscription)\s+(\w+)", text, re.I)
        op_name = op_match.group(1) if op_match else None

        # Extract fields
        field_names = re.findall(r"^\s{2,}(\w+)(?:\s*[\{\(]|$)", text, re.M)

        # Variables block
        variables: dict = {}
        if "variables" in text.lower():
            var_match = re.search(r'"variables"\s*:\s*(\{[^}]+\})', text, re.S)
            if var_match:
                try:
                    variables = json.loads(var_match.group(1))
                except Exception:
                    pass

        # Fragments
        fragments = re.findall(r"fragment\s+(\w+)\s+on\s+(\w+)", text)
        if fragments:
            anomalies.append(Anomaly(severity="info", code="GQL_FRAGMENTS", message=f"{len(fragments)} fragment(s) detected"))

        # Directives
        directives = re.findall(r"@(\w+)", text)
        if "@deprecated" in directives:
            anomalies.append(Anomaly(severity="warn", code="GQL_DEPRECATED", message="Deprecated directive used"))

        # Depth estimation
        depth = max(text.count("{") - text.count("}"), 0) + text.count("{")
        if depth > 10:
            anomalies.append(Anomaly(severity="warn", code="GQL_DEEP_QUERY", message=f"Query depth ~{depth} may cause performance issues"))

        obj = {
            "operation_type": op_type,
            "operation_name": op_name,
            "fields": field_names,
            "variables": variables,
            "fragments": [{"name": n, "on": t} for n, t in fragments],
            "directives": list(set(directives)),
            "raw_query": text,
        }
        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = self._build_schema(obj)
        result.fields = fields
        result.stats = stats
        result.anomalies = anomalies
        result.pretty = text
        result.metadata = {
            "operation_type": op_type,
            "operation_name": op_name,
            "estimated_depth": depth,
            "fragment_count": len(fragments),
            "directive_count": len(directives),
        }
        return result


# ══════════════════════════════════════════════════════════════════════════════
# CSV
# ══════════════════════════════════════════════════════════════════════════════
class CsvProcessor(BaseProcessor):
    FORMAT_ID = "csv"
    FORMAT_LABEL = "CSV / TSV"

    def can_detect(self, raw: bytes) -> bool:
        s = raw.decode(errors="replace")
        return "," in s[:500] and "\n" in s[:500]

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        anomalies: list[Anomaly] = []
        text = raw.decode(errors="replace")

        # Detect delimiter
        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(text[:2048])
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        rows = []
        for i, row in enumerate(reader):
            if i >= 10000:
                anomalies.append(Anomaly(severity="info", code="CSV_TRUNCATED", message="Loaded first 10,000 rows"))
                break
            rows.append(dict(row))

        headers = list(rows[0].keys()) if rows else []
        col_types = _infer_csv_types(rows, headers)

        # Check for missing values
        missing = sum(1 for row in rows for v in row.values() if v in ("", None))
        if missing:
            anomalies.append(Anomaly(severity="warn", code="CSV_MISSING", message=f"{missing} missing value(s) detected"))

        obj = {"headers": headers, "column_types": col_types, "row_count": len(rows), "sample_rows": rows[:5]}
        fields = [
            FieldInfo(path=f"columns.{h}", type=col_types.get(h, "string"), value=None, depth=1)
            for h in headers
        ]
        stats = Statistics(
            total_fields=len(headers),
            max_depth=2,
            null_count=missing,
            string_count=sum(1 for t in col_types.values() if t == "string"),
            number_count=sum(1 for t in col_types.values() if t in ("integer", "float")),
        )

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = _csv_schema(headers, col_types)
        result.fields = fields
        result.stats = stats
        result.anomalies = anomalies
        result.pretty = text[:5000]
        result.metadata = {
            "delimiter": repr(delimiter),
            "row_count": len(rows),
            "column_count": len(headers),
            "columns": headers,
        }
        return result


def _infer_csv_types(rows: list[dict], headers: list[str]) -> dict[str, str]:
    types: dict[str, str] = {}
    for h in headers:
        vals = [r[h] for r in rows[:100] if r.get(h)]
        if not vals:
            types[h] = "null"
            continue
        try:
            [int(v) for v in vals]
            types[h] = "integer"
        except ValueError:
            try:
                [float(v) for v in vals]
                types[h] = "float"
            except ValueError:
                types[h] = "string"
    return types


def _csv_schema(headers: list[str], types: dict[str, str]) -> SchemaNode:
    root = SchemaNode(name="csv", type="table")
    for h in headers:
        root.children.append(SchemaNode(name=h, type=types.get(h, "string")))
    return root


# ══════════════════════════════════════════════════════════════════════════════
# YAML
# ══════════════════════════════════════════════════════════════════════════════
class YamlProcessor(BaseProcessor):
    FORMAT_ID = "yaml"
    FORMAT_LABEL = "YAML"

    def can_detect(self, raw: bytes) -> bool:
        s = raw.decode(errors="replace")
        return s.startswith("---") or re.match(r"^\w+:\s", s)

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        try:
            import yaml  # type: ignore
            obj = yaml.safe_load(raw.decode(errors="replace"))
        except Exception as exc:
            raise ValueError(f"YAML parse error: {exc}") from exc

        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = self._build_schema(obj)
        result.fields = fields[:500]
        result.stats = stats
        result.pretty = json.dumps(obj, indent=2, default=str)
        result.metadata = {"top_level_keys": list(obj.keys()) if isinstance(obj, dict) else []}
        return result


# ══════════════════════════════════════════════════════════════════════════════
# TOML
# ══════════════════════════════════════════════════════════════════════════════
class TomlProcessor(BaseProcessor):
    FORMAT_ID = "toml"
    FORMAT_LABEL = "TOML"

    def can_detect(self, raw: bytes) -> bool:
        s = raw.decode(errors="replace")
        return bool(re.match(r"^\[[\w.]+\]", s, re.M) or re.match(r"^\w+ = ", s))

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore
        try:
            obj = tomllib.loads(raw.decode(errors="replace"))
        except Exception as exc:
            raise ValueError(f"TOML parse error: {exc}") from exc

        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = self._build_schema(obj)
        result.fields = fields[:500]
        result.stats = stats
        result.pretty = json.dumps(obj, indent=2, default=str)
        result.metadata = {"sections": list(obj.keys())}
        return result


# ══════════════════════════════════════════════════════════════════════════════
# Binary / Hex Inspector
# ══════════════════════════════════════════════════════════════════════════════
class BinaryProcessor(BaseProcessor):
    FORMAT_ID = "binary"
    FORMAT_LABEL = "Binary / Hex"

    def can_detect(self, raw: bytes) -> bool:
        # Only claim binary if significant non-printable content AND not a known text format
        sample = raw[:256]
        non_print = sum(1 for b in sample if b < 0x09 or (0x0e <= b <= 0x1f) or b == 0x7f)
        if non_print / max(len(sample), 1) < 0.15:
            return False
        # Don't claim XML, JSON, etc.
        s = raw.lstrip()
        if s[:1] in (b"{", b"[", b"<") or s[:3] == b"---":
            return False
        return True

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        anomalies: list[Anomaly] = []

        # Magic byte detection
        magic = _detect_magic(raw)
        if magic:
            anomalies.append(Anomaly(severity="info", code="MAGIC_BYTES", message=f"Detected file signature: {magic}"))

        # Entropy
        entropy = _byte_entropy(raw)
        if entropy > 7.5:
            anomalies.append(Anomaly(severity="warn", code="HIGH_ENTROPY", message=f"High entropy ({entropy:.2f}) — data may be compressed or encrypted"))

        # Hex dump
        hex_lines = []
        for i in range(0, min(len(raw), 512), 16):
            chunk = raw[i:i+16]
            hex_part = " ".join(f"{b:02x}" for b in chunk)
            ascii_part = "".join(chr(b) if 0x20 <= b < 0x7f else "." for b in chunk)
            hex_lines.append(f"{i:08x}  {hex_part:<47}  |{ascii_part}|")

        # Byte frequency distribution
        freq = [0] * 256
        for b in raw:
            freq[b] += 1

        obj = {
            "size": len(raw),
            "magic": magic,
            "entropy": entropy,
            "byte_frequencies": {hex(i): freq[i] for i in range(256) if freq[i] > 0},
            "null_bytes": raw.count(b"\x00"),
            "printable_ratio": sum(1 for b in raw if 0x20 <= b < 0x7f) / max(len(raw), 1),
        }

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = SchemaNode(name="binary", type="binary")
        result.fields = [
            FieldInfo(path="size", type="int", value=len(raw), depth=0),
            FieldInfo(path="entropy", type="float", value=entropy, depth=0),
            FieldInfo(path="magic", type="str", value=magic, depth=0),
        ]
        result.stats = Statistics(total_fields=3)
        result.anomalies = anomalies
        result.pretty = "\n".join(hex_lines)
        result.metadata = {
            "file_type": magic or "unknown",
            "entropy": entropy,
            "size_bytes": len(raw),
        }
        return result


def _detect_magic(raw: bytes) -> str | None:
    MAGIC: dict[bytes, str] = {
        b"\x89PNG\r\n\x1a\n": "PNG Image",
        b"\xff\xd8\xff": "JPEG Image",
        b"GIF8": "GIF Image",
        b"PK\x03\x04": "ZIP Archive",
        b"\x1f\x8b": "Gzip Compressed",
        b"BZh": "Bzip2 Compressed",
        b"\x28\xb5\x2f\xfd": "Zstandard Compressed",
        b"%PDF": "PDF Document",
        b"\x7fELF": "ELF Executable",
        b"MZ": "PE Executable (Windows)",
        b"\xca\xfe\xba\xbe": "Mach-O Binary",
        b"OggS": "Ogg Container",
        b"fLaC": "FLAC Audio",
        b"RIFF": "RIFF Container (WAV/AVI)",
        b"\x00\x00\x00\x20ftyp": "MP4 Video",
        b"\x1a\x45\xdf\xa3": "MKV Video",
        b"SQLite format 3": "SQLite Database",
    }
    for sig, name in MAGIC.items():
        if raw[:len(sig)] == sig:
            return name
    return None


def _byte_entropy(data: bytes) -> float:
    import math
    if not data:
        return 0.0
    freq = [0] * 256
    for b in data:
        freq[b] += 1
    total = len(data)
    return -sum((c / total) * math.log2(c / total) for c in freq if c)


# ══════════════════════════════════════════════════════════════════════════════
# HAR (HTTP Archive)
# ══════════════════════════════════════════════════════════════════════════════
class HarProcessor(BaseProcessor):
    FORMAT_ID = "har"
    FORMAT_LABEL = "HAR (HTTP Archive)"

    def can_detect(self, raw: bytes) -> bool:
        try:
            obj = orjson.loads(raw)
            return "log" in obj and "entries" in obj.get("log", {})
        except Exception:
            return False

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        obj = orjson.loads(raw)
        log = obj["log"]
        entries = log.get("entries", [])
        anomalies: list[Anomaly] = []

        # Aggregate stats
        methods: dict[str, int] = {}
        status_groups: dict[str, int] = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0}
        timings: list[float] = []
        sizes: list[int] = []

        for e in entries:
            req = e.get("request", {})
            resp = e.get("response", {})
            m = req.get("method", "?")
            methods[m] = methods.get(m, 0) + 1
            s = resp.get("status", 0)
            grp = f"{s // 100}xx"
            if grp in status_groups:
                status_groups[grp] += 1
            t = e.get("time", 0)
            if t:
                timings.append(t)
            sz = resp.get("bodySize", -1)
            if sz >= 0:
                sizes.append(sz)

        if status_groups["4xx"] + status_groups["5xx"] > 0:
            anomalies.append(Anomaly(severity="warn", code="HAR_ERRORS", message=f"{status_groups['4xx']} client errors, {status_groups['5xx']} server errors"))

        slow = [t for t in timings if t > 3000]
        if slow:
            anomalies.append(Anomaly(severity="warn", code="HAR_SLOW", message=f"{len(slow)} request(s) > 3s"))

        summary = {
            "creator": log.get("creator", {}),
            "entry_count": len(entries),
            "methods": methods,
            "status_groups": status_groups,
            "avg_time_ms": round(sum(timings) / len(timings), 2) if timings else 0,
            "p95_time_ms": round(sorted(timings)[int(len(timings) * 0.95)] if timings else 0, 2),
            "total_bytes": sum(sizes),
            "pages": len(log.get("pages", [])),
        }

        fields = self._collect_fields(summary)
        stats = self._compute_stats(summary, fields)

        result = self._base_result(raw, t0)
        result.parsed = summary
        result.schema_tree = self._build_schema(summary)
        result.fields = fields
        result.stats = stats
        result.anomalies = anomalies
        result.pretty = json.dumps(summary, indent=2)
        result.metadata = summary
        return result


# ══════════════════════════════════════════════════════════════════════════════
# Protobuf (schema-less decode)
# ══════════════════════════════════════════════════════════════════════════════
class ProtobufProcessor(BaseProcessor):
    FORMAT_ID = "protobuf"
    FORMAT_LABEL = "Protocol Buffers"

    def can_detect(self, raw: bytes) -> bool:
        # Proto wire: field tag varint, check for valid wire types
        if len(raw) < 2:
            return False
        b = raw[0]
        wire = b & 0x07
        return wire in (0, 1, 2, 5) and (b >> 3) > 0

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        obj = _proto_decode(raw)

        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = self._build_schema(obj)
        result.fields = fields
        result.stats = stats
        result.pretty = json.dumps(obj, indent=2, default=str)
        result.metadata = {"wire_fields": len(obj)}
        result.anomalies = [Anomaly(severity="info", code="PROTO_NO_SCHEMA", message="Decoded without .proto schema — field names are numeric")]
        return result


def _proto_decode(data: bytes) -> dict:
    """Schema-less protobuf wire decoder."""
    fields: dict[str, Any] = {}
    i = 0
    while i < len(data):
        try:
            tag_wire, n = _read_varint(data, i)
            i += n
            field_num = tag_wire >> 3
            wire_type = tag_wire & 0x07
            key = f"field_{field_num}"

            if wire_type == 0:  # varint
                val, n = _read_varint(data, i)
                i += n
                fields[key] = val
            elif wire_type == 1:  # 64-bit
                fields[key] = struct.unpack_from("<Q", data, i)[0]
                i += 8
            elif wire_type == 2:  # length-delimited
                ln, n = _read_varint(data, i)
                i += n
                chunk = data[i:i+ln]
                i += ln
                # Try as UTF-8 string first
                try:
                    fields[key] = chunk.decode("utf-8")
                except Exception:
                    fields[key] = chunk.hex()
            elif wire_type == 5:  # 32-bit
                fields[key] = struct.unpack_from("<I", data, i)[0]
                i += 4
            else:
                break
        except Exception:
            break
    return fields


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    read = 0
    while True:
        b = data[pos + read]
        result |= (b & 0x7F) << shift
        read += 1
        shift += 7
        if not (b & 0x80):
            break
        if read > 10:
            raise ValueError("Varint too long")
    return result, read
