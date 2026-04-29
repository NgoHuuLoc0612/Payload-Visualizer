# ⬡ Payload Visualizer

**Multi-format payload analysis & visualization platform.**

Parse, inspect, diff, transform, and stream-analyze API payloads in real time — directly from the browser.

---

## ✨ Features

- **Multi-format parsing** — JSON, XML/SOAP, MessagePack, JWT/JWS, GraphQL, CSV/TSV, YAML, TOML, Protobuf (schema-less), Binary/Hex, HAR
- **Auto-detection** — heuristic format detection from raw bytes, no manual selection needed
- **Schema tree** — recursive schema visualization with types, nullability, and nesting
- **Statistics** — field counts, max depth, entropy, null/array/object distribution
- **Anomaly detection** — catches JWT expiry, deep nesting, duplicate keys, missing CSV values, high binary entropy, and more
- **Diff engine** — structural diff between two payloads of the same format (added / removed / modified)
- **Transform** — convert payloads between formats (JSON ↔ YAML ↔ TOML ↔ MsgPack ↔ CSV)
- **Validate** — validate against a JSON Schema
- **Query** — apply jq-style expressions to JSON payloads
- **File upload** — parse files directly via multipart upload
- **Real-time WebSocket stream** — live metrics dashboard with batched events, system snapshots, and broadcast mode
- **Dark/light theme** — toggleable UI with CodeMirror editor integration

---

## 🗂 Project Structure

```
Payload-Visualizer/
├── main.py                  # FastAPI app entry point, lifespan, middleware
├── requirements.txt         # Python dependencies
├── core/
│   ├── config.py            # Pydantic settings (env vars, .env support)
│   └── registry.py          # FormatRegistry — discovers & dispatches processors
├── processors/
│   ├── base.py              # BaseProcessor ABC, ParseResult, FieldInfo, Statistics models
│   └── formats.py           # All format processors (JSON, XML, JWT, GraphQL, CSV, ...)
├── api/
│   ├── routes.py            # REST API endpoints (/parse, /diff, /transform, /validate, ...)
│   └── websocket.py         # WebSocket handlers (/ws/stream, /ws/broadcast)
└── static/
    ├── index.html           # Single-page application shell
    ├── css/app.css          # Styles
    └── js/app.js            # Frontend modules (API, WS, Editor, Charts, Diff, History)
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/payload-visualizer.git
cd payload-visualizer

# Install dependencies
pip install -r requirements.txt
```

### Run

```bash
python main.py
```

The server starts at **http://localhost:8000** by default.

- **UI:** http://localhost:8000
- **API Docs (Swagger):** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **WebSocket stream:** ws://localhost:8000/ws/stream

---

## ⚙️ Configuration

Configuration is managed via environment variables (prefix `PV_`) or a `.env` file:

| Variable | Default | Description |
|---|---|---|
| `PV_HOST` | `0.0.0.0` | Bind host |
| `PV_PORT` | `8000` | Bind port |
| `PV_DEBUG` | `false` | Enable hot-reload (development) |
| `PV_MAX_PAYLOAD_BYTES` | `10485760` | Max payload size (10 MB) |
| `PV_WS_HEARTBEAT_INTERVAL` | `15.0` | WebSocket heartbeat in seconds |
| `PV_STREAM_BATCH_SIZE` | `50` | Events per WebSocket batch |
| `PV_STREAM_INTERVAL_MS` | `250` | Streaming interval in milliseconds |

Example `.env`:

```env
PV_PORT=9000
PV_DEBUG=true
PV_MAX_PAYLOAD_BYTES=20971520
```

---

## 📡 REST API

All endpoints are prefixed with `/api/v1`.

### `GET /api/v1/formats`
Returns all registered format processors.

### `POST /api/v1/parse`
Parse a payload and return full analysis.

```json
{
  "payload": "{\"hello\": \"world\"}",
  "format": "auto",
  "encoding": "utf-8",
  "base64_input": false
}
```

### `POST /api/v1/parse/file`
Parse an uploaded file via multipart form (`file` field). Accepts `?format=auto`.

### `POST /api/v1/detect`
Detect the format of a payload without full parsing.

### `POST /api/v1/diff`
Structural diff between two payloads.

```json
{
  "left": "{\"a\": 1}",
  "right": "{\"a\": 2, \"b\": 3}",
  "format": "json"
}
```

### `POST /api/v1/transform`
Transform a payload from one format to another.

```json
{
  "payload": "{\"name\": \"Alice\"}",
  "source_format": "json",
  "target_format": "yaml"
}
```

Supported targets: `json`, `yaml`, `toml`, `msgpack`, `csv`.

### `POST /api/v1/validate`
Validate a payload, optionally against a JSON Schema.

### `POST /api/v1/query`
Apply a jq-style dot-notation expression to a parsed payload.

### `GET /api/v1/health`
Health check — returns `{"status": "ok"}`.

---

## 🔌 WebSocket API

### `ws://host/ws/stream`

Bidirectional real-time stream. The server pushes metric batches and system snapshots automatically. Clients send JSON commands:

| Command | Description |
|---|---|
| `{"cmd": "ping"}` | Heartbeat ping |
| `{"cmd": "subscribe", "interval_ms": 250, "batch_size": 20}` | Adjust stream rate |
| `{"cmd": "parse", "payload": "...", "format": "json"}` | Parse a payload over WS |
| `{"cmd": "history", "n": 50}` | Retrieve last N recorded events |
| `{"cmd": "pause"}` | Pause the metric stream |
| `{"cmd": "resume"}` | Resume the metric stream |

### `ws://host/ws/broadcast`

Send a payload; parsed result is broadcast to all connected clients.

---

## 🧩 Adding a New Format Processor

1. Open `processors/formats.py`.
2. Create a class extending `BaseProcessor`:

```python
class MyFormatProcessor(BaseProcessor):
    FORMAT_ID = "myformat"
    FORMAT_LABEL = "My Format"

    def can_detect(self, raw: bytes) -> bool:
        # Return True if raw bytes look like your format
        return raw[:4] == b"MYFM"

    def parse(self, raw: bytes) -> ParseResult:
        t0 = _now_ms()
        # Parse the raw bytes into a Python object
        obj = my_parse_function(raw)

        fields = self._collect_fields(obj)
        stats = self._compute_stats(obj, fields)

        result = self._base_result(raw, t0)
        result.parsed = obj
        result.schema_tree = self._build_schema(obj)
        result.fields = fields[:500]
        result.stats = stats
        result.pretty = str(obj)
        return result
```

3. The `FormatRegistry` auto-discovers any class with a `FORMAT_ID` attribute in the `processors` package on startup.

---

## 📦 Dependencies

| Package | Purpose |
|---|---|
| `fastapi` | Web framework |
| `uvicorn` | ASGI server |
| `pydantic` / `pydantic-settings` | Data models & configuration |
| `orjson` | Fast JSON serialization |
| `msgpack` | MessagePack decoding |
| `structlog` | Structured logging |
| `rich` | Terminal output formatting |
| `pyyaml` | YAML parsing |
| `tomli` | TOML parsing (Python < 3.11) |
| `protobuf` | Protobuf support |
| `numpy` / `pandas` / `scipy` | Statistical analysis helpers |
| `websockets` | WebSocket support |
| `httpx` | Async HTTP client |

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
