"""
Application settings — loaded from environment / .env file.
Usage: from src.utils.config import settings
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── LLM ──
    anthropic_api_key:   Optional[str] = None
    google_api_key:      Optional[str] = None
    primary_llm_provider: str = "claude"
    primary_model:        str = "claude-sonnet-4-5-20250929"
    fallback_llm_provider: str = "gemini"
    fallback_model:        str = "gemini-2.5-flash"

    # ── Pipeline ──
    max_patch_retries:         int = 3
    compile_timeout_seconds:   int = 120
    test_timeout_seconds:      int = 300

    # ── Tool paths ──
    mvn_path:    str = "mvn"
    gradle_path: str = "gradle"
    scalac_path: str = "scalac"
    
    # ── LangSmith Tracing (optional) ──
    langchain_tracing_v2:  Optional[str] = None
    langchain_api_key:     Optional[str] = None
    langchain_project:     str = "mr-spark-migration"
    langchain_endpoint:    str = "https://api.smith.langchain.com"

    # ── Streamlit ──
    streamlit_port: int = 8501

    # ── Logging ──
    log_level:  str = "INFO"
    log_format: str = "console"

    @field_validator("primary_llm_provider", "fallback_llm_provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        allowed = {"claude", "gemini"}
        if v.lower() not in allowed:
            raise ValueError(f"LLM provider must be one of {allowed}, got '{v}'")
        return v.lower()

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR"}
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of {allowed}")
        return v

    @property
    def has_anthropic(self) -> bool:
        return bool(self.anthropic_api_key)

    @property
    def has_google(self) -> bool:
        return bool(self.google_api_key)

    @property
    def effective_provider(self) -> str:
        """Return primary if key is available, else fallback."""
        if self.primary_llm_provider == "claude" and self.has_anthropic:
            return "claude"
        if self.primary_llm_provider == "gemini" and self.has_google:
            return "gemini"
        # try fallback
        if self.fallback_llm_provider == "claude" and self.has_anthropic:
            return "claude"
        if self.fallback_llm_provider == "gemini" and self.has_google:
            return "gemini"
        return self.primary_llm_provider  # will fail at runtime if no key


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# Convenience singleton
settings = get_settings()
