"""Core storage layer for context-system.

Provides SQLite-backed project context and session transcript management.
"""
from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from hashlib import sha256
from .config import DB_PATH, DB_DIR, SESSIONS_DIR, CACHE_DIR, TIMEZONE


class ContextStorage:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path or DB_PATH)
        self._ensure_dirs()
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _ensure_dirs(self) -> None:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _init_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS project_context (
            id INTEGER PRIMARY KEY,
            category TEXT,
            content TEXT,
            embedding TEXT,
            created_at TEXT,
            updated_at TEXT,
            tags TEXT
        );
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            project_id TEXT,
            transcript_path TEXT,
            summary TEXT,
            start_time TEXT,
            end_time TEXT,
            model_used TEXT,
            token_cost REAL
        );
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS context_cache (
            hash TEXT PRIMARY KEY,
            cached_content TEXT,
            provider TEXT,
            expires_at TEXT,
            hit_count INTEGER,
            metadata TEXT
        );
        """
        )

        # Backwards-compatible column additions
        try:
            cur.execute("ALTER TABLE sessions ADD COLUMN token_count INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE context_cache ADD COLUMN metadata TEXT")
        except Exception:
            pass

        self.conn.commit()

    def save_project_context(
        self, category: str, content: str, embedding: Optional[List[float]] = None, tags: Optional[Dict[str, Any]] = None
    ) -> int:
        now = datetime.now(TIMEZONE).isoformat()
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO project_context (category, content, embedding, created_at, updated_at, tags) VALUES (?, ?, ?, ?, ?, ?)",
            (
                category,
                content,
                json.dumps(embedding) if embedding is not None else None,
                now,
                now,
                json.dumps(tags) if tags else None,
            ),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_project_context(self, id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM project_context WHERE id = ?", (id,))
        row = cur.fetchone()
        if not row:
            return None
        return {k: row[k] for k in row.keys()}

    def append_session_message(self, session_id: str, message: Dict[str, Any]) -> None:
        session_file = Path(SESSIONS_DIR) / f"{session_id}.jsonl"
        with session_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(message, ensure_ascii=False) + "\n")

    def create_session(self, session_id: str, project_id: Optional[str] = None, model_used: Optional[str] = None) -> None:
        start_time = datetime.now(TIMEZONE).isoformat()
        transcript_path = str(Path(SESSIONS_DIR) / f"{session_id}.jsonl")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO sessions (id, project_id, transcript_path, summary, start_time, end_time, model_used, token_cost) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (session_id, project_id, transcript_path, None, start_time, None, model_used, 0.0),
        )
        self.conn.commit()

    def end_session(self, session_id: str, summary: Optional[str] = None, token_cost: float = 0.0) -> None:
        end_time = datetime.now(TIMEZONE).isoformat()
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE sessions SET summary = ?, end_time = ?, token_cost = ? WHERE id = ?",
            (summary, end_time, token_cost, session_id),
        )
        self.conn.commit()

    def set_session_summary(self, session_id: str, summary: Optional[str] = None, token_cost: Optional[float] = None, set_end_time: bool = False) -> None:
        """Set or update the session summary without necessarily ending the session.

        If `set_end_time` is True, the session's end_time will be set to now().
        """
        cur = self.conn.cursor()
        params = []
        fields = []
        if summary is not None:
            fields.append("summary = ?")
            params.append(summary)
        if token_cost is not None:
            fields.append("token_cost = ?")
            params.append(token_cost)
        if set_end_time:
            fields.append("end_time = ?")
            params.append(datetime.now(TIMEZONE).isoformat())
        if not fields:
            return
        params.append(session_id)
        sql = f"UPDATE sessions SET {', '.join(fields)} WHERE id = ?"
        cur.execute(sql, tuple(params))
        self.conn.commit()

    def load_session_transcript(self, session_id: str) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT transcript_path FROM sessions WHERE id = ?", (session_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return []
        transcript_path = Path(row[0])
        if not transcript_path.exists():
            return []
        messages = []
        with transcript_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    messages.append(json.loads(line))
                except Exception:
                    continue
        return messages

    def summarize_session_placeholder(self, session_id: str) -> str:
        # Placeholder summarization: load transcript and create a short extractive summary.
        messages = self.load_session_transcript(session_id)
        if not messages:
            return ""
        # Simple heuristic: take first and last user messages and join.
        first = messages[0].get("text") if messages else ""
        last = messages[-1].get("text") if messages else ""
        summary = (first or "")[:500] + "\n---\n" + (last or "")[:500]
        # Save summary into sessions table
        self.end_session(session_id, summary=summary, token_cost=0.0)
        return summary

    def cache_prefix(self, prefix: str, provider: str, content: str, ttl_seconds: int = 3600) -> str:
        return self.cache_prefix_with_hints(prefix, provider, content, ttl_seconds, cache_hints=None)
        

    def cache_prefix_with_hints(self, prefix: str, provider: str, content: str, ttl_seconds: int = 3600, cache_hints: Optional[dict] = None) -> str:
        """Cache a prefix with optional provider-specific cache hints stored in metadata."""
        h = sha256(prefix.encode("utf-8")).hexdigest()
        expires = (datetime.now(TIMEZONE).timestamp() + ttl_seconds)
        cur = self.conn.cursor()
        metadata = None
        if cache_hints:
            try:
                metadata = json.dumps(cache_hints)
            except Exception:
                metadata = json.dumps({"hint": str(cache_hints)})
        cur.execute(
            "INSERT OR REPLACE INTO context_cache (hash, cached_content, provider, expires_at, hit_count, metadata) VALUES (?, ?, ?, ?, ?, ?)",
            (h, content, provider, str(expires), 0, metadata),
        )
        self.conn.commit()
        return h

    def get_cached_prefix(self, prefix: str) -> Optional[Dict[str, Any]]:
        h = sha256(prefix.encode("utf-8")).hexdigest()
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM context_cache WHERE hash = ?", (h,))
        row = cur.fetchone()
        if not row:
            return None
        # Check expiry
        try:
            expires = float(row["expires_at"]) if row["expires_at"] else 0.0
        except Exception:
            expires = 0.0
        if expires and datetime.now(TIMEZONE).timestamp() > expires:
            return None
        # bump hit count
        cur.execute("UPDATE context_cache SET hit_count = hit_count + 1 WHERE hash = ?", (h,))
        self.conn.commit()
        out = {k: row[k] for k in row.keys()}
        # parse metadata if present
        try:
            out["metadata"] = json.loads(out.get("metadata")) if out.get("metadata") else None
        except Exception:
            out["metadata"] = out.get("metadata")
        return out

    def update_project_context_embedding(self, context_id: int, embedding: List[float]) -> None:
        """Update the stored embedding for a project_context row."""
        cur = self.conn.cursor()
        now = datetime.now(TIMEZONE).isoformat()
        cur.execute(
            "UPDATE project_context SET embedding = ?, updated_at = ? WHERE id = ?",
            (json.dumps(embedding), now, context_id),
        )
        self.conn.commit()

    def list_project_contexts(self) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM project_context ORDER BY created_at DESC")
        rows = cur.fetchall()
        return [{k: row[k] for k in row.keys()} for row in rows]

    def add_session_token_cost(self, session_id: str, delta_cost: float, delta_tokens: Optional[int] = None) -> None:
        cur = self.conn.cursor()
        # ensure session exists
        cur.execute("SELECT token_cost, token_count FROM sessions WHERE id = ?", (session_id,))
        row = cur.fetchone()
        if not row:
            return
        try:
            prev_cost = float(row[0]) if row[0] is not None else 0.0
        except Exception:
            prev_cost = 0.0
        try:
            prev_tokens = int(row[1]) if row[1] is not None else 0
        except Exception:
            prev_tokens = 0
        new_cost = prev_cost + float(delta_cost)
        new_tokens = prev_tokens + (int(delta_tokens) if delta_tokens else 0)
        cur.execute("UPDATE sessions SET token_cost = ?, token_count = ? WHERE id = ?", (new_cost, new_tokens, session_id))
        self.conn.commit()

