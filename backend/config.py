from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _text(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _number(name: str, default: float) -> float:
    raw = _text(name, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def _integer(name: str, default: int) -> int:
    raw = _text(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _flag(name: str, default: bool) -> bool:
    raw = _text(name, "true" if default else "false").lower()
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    mock_mode: bool
    databricks_host: str
    databricks_token: str
    databricks_http_path: str
    databricks_job_id: str
    databricks_catalog: str
    databricks_schema: str
    raw_volume: str
    cache_ttl_seconds: int
    local_cache_db: Path
    raw_data_dir: Path
    notebooks_dir: Path
    web_dir: Path
    llm_provider: str
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    llm_timeout_seconds: int
    agent_max_tool_rounds: int

    @property
    def sql_http_path(self) -> str:
        if self.databricks_http_path.startswith("/sql/"):
            return self.databricks_http_path
        return f"/sql/1.0/warehouses/{self.databricks_http_path}"

    @property
    def sql_server_hostname(self) -> str:
        return (
            self.databricks_host.replace("https://", "")
            .replace("http://", "")
            .strip("/")
        )

    @property
    def llm_ready(self) -> bool:
        if self.llm_provider.lower() in {"disabled", "none", "off"}:
            return False
        if not self.llm_base_url or not self.llm_model:
            return False
        if self.llm_api_key:
            return True
        if self.llm_provider.lower() == "ollama":
            return True
        return "localhost" in self.llm_base_url or "127.0.0.1" in self.llm_base_url

    @property
    def llm_label(self) -> str:
        if self.llm_ready:
            return f"{self.llm_provider}:{self.llm_model}"
        return "deterministic-fallback"

    @property
    def mode_label(self) -> str:
        return "MOCK" if self.mock_mode else "DATABRICKS_LIVE"

    @property
    def raw_volume_path(self) -> str:
        return f"{self.raw_volume.rstrip('/')}"


def _resolve_path(name: str, default: str) -> Path:
    raw = _text(name, default)
    path = Path(raw)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    cache_path = _text("LOCAL_CACHE_DB") or _text("LOCAL_CACHE_PATH", "backend/local_cache.db")
    local_cache = Path(cache_path)
    if not local_cache.is_absolute():
        local_cache = BASE_DIR / local_cache
    local_cache.parent.mkdir(parents=True, exist_ok=True)
    settings = Settings(
        mock_mode=_flag("MOCK_MODE", True),
        databricks_host=_text("DATABRICKS_HOST"),
        databricks_token=_text("DATABRICKS_TOKEN"),
        databricks_http_path=_text(
            "DATABRICKS_HTTP_PATH",
            _text("DATABRICKS_WAREHOUSE_ID", ""),
        ),
        databricks_job_id=_text("DATABRICKS_JOB_ID", "0"),
        databricks_catalog=_text("DATABRICKS_CATALOG", "workspace"),
        databricks_schema=_text("DATABRICKS_SCHEMA", "claims_ops"),
        raw_volume=_text("DATABRICKS_RAW_VOLUME", "/Volumes/workspace/claims_ops/raw_landing"),
        cache_ttl_seconds=_integer("CACHE_TTL_SECONDS", 60),
        local_cache_db=local_cache,
        raw_data_dir=_resolve_path("RAW_DATA_DIR", "data/raw"),
        notebooks_dir=BASE_DIR / "databricks_notebooks",
        web_dir=BASE_DIR / "web",
        llm_provider=_text("LLM_PROVIDER", "openai"),
        llm_base_url=_text("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
        llm_model=_text("LLM_MODEL", "gpt-4o-mini"),
        llm_api_key=_text("LLM_API_KEY"),
        llm_timeout_seconds=_integer("LLM_TIMEOUT_SECONDS", 45),
        agent_max_tool_rounds=_integer("AGENT_MAX_TOOL_ROUNDS", 6),
    )
    return settings


def public_config() -> dict[str, object]:
    settings = get_settings()
    return {
        "mock_mode": settings.mock_mode,
        "mode": settings.mode_label,
        "catalog": settings.databricks_catalog,
        "schema": settings.databricks_schema,
        "sql_http_path_configured": bool(settings.databricks_http_path),
        "job_id_configured": settings.databricks_job_id not in {"", "0"},
        "llm_provider": settings.llm_provider,
        "llm_model": settings.llm_model,
        "llm_ready": settings.llm_ready,
        "cache_ttl_seconds": settings.cache_ttl_seconds,
        "local_cache_db": str(settings.local_cache_db),
    }
