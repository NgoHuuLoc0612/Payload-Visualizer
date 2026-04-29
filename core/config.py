from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PV_", env_file=".env", extra="ignore")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    debug: bool = Field(default=False)
    max_payload_bytes: int = Field(default=10 * 1024 * 1024)   # 10 MB
    ws_heartbeat_interval: float = Field(default=15.0)
    stream_batch_size: int = Field(default=50)
    stream_interval_ms: int = Field(default=250)
