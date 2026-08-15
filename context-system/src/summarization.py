"""Summarization utilities for context-system.

Includes a lightweight extractive summarizer (local) and a hook to call
OpenRouter for model-based summarization.
"""
from __future__ import annotations

from typing import List, Optional
import re
from heapq import nlargest
from .storage import ContextStorage
from .config import OPENROUTER_DEFAULTS
import httpx
import os


def _split_sentences(text: str) -> List[str]:
    # naive sentence splitter
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    return [p.strip() for p in parts if p.strip()]


def _score_sentences(sentences: List[str]) -> List[float]:
    # simple frequency-based scoring
    words = []
    for s in sentences:
        words.extend(re.findall(r"\w+", s.lower()))
    freq = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    scores = []
    for s in sentences:
        sc = 0
        for w in re.findall(r"\w+", s.lower()):
            sc += freq.get(w, 0)
        scores.append(sc / (len(re.findall(r"\w+", s)) + 1))
    return scores


def extractive_summarize(text: str, compression: float = 0.1, min_sentences: int = 1) -> str:
    """Extractive summarizer: selects top sentences to meet compression ratio.

    compression: fraction of original length to keep (e.g., 0.1 keeps 10%).
    """
    if not text or not text.strip():
        return ""
    sentences = _split_sentences(text)
    if len(sentences) <= min_sentences:
        return text
    scores = _score_sentences(sentences)
    # target number of sentences
    target = max(min_sentences, int(len(sentences) * compression))
    target = min(target, len(sentences))
    # select top-scoring sentences and preserve original order
    top_idx = set(i for i, _ in nlargest(target, enumerate(scores), key=lambda x: x[1]))
    selected = [s for i, s in enumerate(sentences) if i in top_idx]
    return " ".join(selected)


async def summarize_with_openrouter(text: str, model: str = "openrouter/auto", max_tokens: int = 512) -> str:
    """Call OpenRouter-style endpoint to summarize text (async).

    Expects environment variable OPENROUTER_API_KEY to be set.
    This is a thin wrapper; real implementations should use robust retry/backoff.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY") or OPENROUTER_DEFAULTS.get("api_key")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set for remote summarization")
    url = OPENROUTER_DEFAULTS.get("base_url").rstrip("/") + "/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    prompt = f"Summarize the following content concisely (max {max_tokens} tokens):\n\n{text}"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
        # naive extraction
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            return json.dumps(data)


def summarize_session(session_id: str, storage: Optional[ContextStorage] = None, compression: float = 0.1) -> str:
    storage = storage or ContextStorage()
    msgs = storage.load_session_transcript(session_id)
    if not msgs:
        return ""
    # join user and assistant texts
    texts = []
    for m in msgs:
        t = m.get("text") or m.get("content") or ""
        if t:
            texts.append(t)
    full = "\n\n".join(texts)
    # local extractive summarization
    summary = extractive_summarize(full, compression=compression, min_sentences=2)
    # store summary in sessions table without ending session
    storage.set_session_summary(session_id, summary=summary, set_end_time=False)
    return summary
