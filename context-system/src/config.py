from pathlib import Path
from zoneinfo import ZoneInfo
import os

ROOT = Path(__file__).resolve().parents[1]
CONTEXT_STORE_DIR = ROOT / "context_store"
DB_DIR = CONTEXT_STORE_DIR / "db"
DB_PATH = DB_DIR / "context_store.db"
SESSIONS_DIR = CONTEXT_STORE_DIR / "sessions"
CACHE_DIR = CONTEXT_STORE_DIR / "cache"

# Timezone for timestamps (IST)
TIMEZONE = ZoneInfo("Asia/Kolkata")

# Embedding size expected by the system. Stored as JSON by default.
EMBEDDING_DIM = 1536

# OpenRouter defaults (placeholders)
OPENROUTER_DEFAULTS = {
    "base_url": os.environ.get("OPENROUTER_URL", "https://api.openrouter.ai"),
    "api_key": os.environ.get("OPENROUTER_API_KEY", ""),
}
