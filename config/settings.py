"""
config/settings.py
Centralised application settings loaded from .env file.
All components import settings from here — never read os.environ directly.
"""
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Paths ────────────────────────────────────────────────────────────
    duckdb_path: str = "data/alphalens.duckdb"
    sqlite_path: str = "data/alphalens.db"
    models_dir:  str = "alphalens/models"
    logs_dir:    str = "alphalens/logs"
    exports_dir: str = "alphalens/exports"

    # ── Kite ─────────────────────────────────────────────────────────────
    kite_api_key:      Optional[str] = None
    kite_api_secret:   Optional[str] = None
    kite_access_token: Optional[str] = None
    kite_user_id:      Optional[str] = None
    kite_password:     Optional[str] = None
    kite_totp_secret:  Optional[str] = None

    # ── Telegram ─────────────────────────────────────────────────────────
    telegram_bot_token: Optional[str] = None
    telegram_chat_id:   Optional[str] = None

    # ── Email ─────────────────────────────────────────────────────────────
    email_smtp_host: str = "smtp.gmail.com"
    email_smtp_port: int = 587
    email_address:   Optional[str] = None
    email_password:  Optional[str] = None
    email_to:        Optional[str] = None

    # ── App ───────────────────────────────────────────────────────────────
    app_env:         str = "development"
    log_level:       str = "INFO"
    dashboard_port:  int = 8050
    dashboard_host:  str = "127.0.0.1"

    # ── Capital ───────────────────────────────────────────────────────────
    total_capital:    float = 2_500_000.0
    intraday_capital: float = 250_000.0
    swing_capital:    float = 500_000.0
    medium_capital:   float = 750_000.0
    longterm_capital: float = 1_000_000.0

    # ── Portfolio Slots ───────────────────────────────────────────────────
    intraday_slots: int = 3
    swing_slots:    int = 5
    medium_slots:   int = 8
    longterm_slots: int = 15

    # ── ML Thresholds ─────────────────────────────────────────────────────
    signal_threshold_bull:    float = 0.65
    signal_threshold_neutral: float = 0.75
    signal_threshold_bear:    float = 0.85
    min_risk_reward:          float = 1.5

    # ── Derived properties ────────────────────────────────────────────────
    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def slot_config(self) -> dict:
        return {
            "intraday":  self.intraday_slots,
            "swing":     self.swing_slots,
            "medium":    self.medium_slots,
            "long_term": self.longterm_slots,
        }

    @property
    def capital_config(self) -> dict:
        return {
            "intraday":  self.intraday_capital,
            "swing":     self.swing_capital,
            "medium":    self.medium_capital,
            "long_term": self.longterm_capital,
        }

    @property
    def signal_thresholds(self) -> dict:
        return {
            "bull":    self.signal_threshold_bull,
            "neutral": self.signal_threshold_neutral,
            "bear":    self.signal_threshold_bear,
        }

    def ensure_dirs(self):
        """Create all required directories if they don't exist."""
        for path in [
            self.duckdb_path, self.sqlite_path,
            self.models_dir, self.logs_dir, self.exports_dir
        ]:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        for d in [self.models_dir, self.logs_dir, self.exports_dir]:
            Path(d).mkdir(parents=True, exist_ok=True)


# Singleton — import this everywhere
settings = Settings()
