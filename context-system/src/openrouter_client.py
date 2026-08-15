"""Simple OpenRouter-compatible client with model routing and cache hooks.

This is a lightweight, provider-agnostic wrapper that supports a fallback
chain and uses the storage cache for prefix caching.
"""
from __future__ import annotations

from typing import List, Dict, Any, Optional
import os
import httpx
from .config import OPENROUTER_DEFAULTS
from .storage import ContextStorage
import time

# Simple default cost per 1k tokens for illustrative accounting (INR)
DEFAULT_COST_PER_1K = {
    "openrouter/auto": 0.02,  # placeholder per 1k tokens in INR
    "openrouter/cheap": 0.005,
}


class OpenRouterClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None, storage: Optional[ContextStorage] = None):
        self.base_url = (base_url or OPENROUTER_DEFAULTS.get("base_url", "https://api.openrouter.ai")).rstrip("/")
        self.api_key = api_key or OPENROUTER_DEFAULTS.get("api_key") or os.environ.get("OPENROUTER_API_KEY")
        self.storage = storage or ContextStorage()

    def _route_models(self, model_hint: str) -> List[str]:
        """Return a prioritized list of model ids based on a hint like 'openrouter/auto' or 'deepseek/flash'."""
        # naive routing rules; real system should be configurable
        if model_hint.startswith("openrouter/"):
            return [model_hint, "openrouter/cheap"]
        # accept plain model names
        return [model_hint, "openrouter/auto"]

    def _post(self, endpoint: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        r = httpx.post(url, json=payload, headers=headers, timeout=30.0)
        r.raise_for_status()
        return r.json()

    def _estimate_tokens(self, messages: List[Dict[str, str]]) -> int:
        # very rough estimate: 1 token ≈ 4 chars
        total_chars = 0
        for m in messages:
            total_chars += len(m.get("content") or m.get("text") or "")
        return max(1, int(total_chars / 4))

    def _estimate_cost(self, model: str, tokens: int) -> float:
        per_k = DEFAULT_COST_PER_1K.get(model, DEFAULT_COST_PER_1K.get("openrouter/auto", 0.02))
        return (tokens / 1000.0) * per_k

    def send_chat(self, messages: List[Dict[str, str]], model: str = "openrouter/auto", max_tokens: int = 1024, cache_prefix: Optional[str] = None, session_id: Optional[str] = None, cache_hints: Optional[dict] = None) -> Dict[str, Any]:
        """Send a chat request, trying fallbacks on failure. Returns response JSON.

        If `cache_prefix` is provided, attempt to return cached content from storage first.
        """
        # prefix cache lookup
        if cache_prefix:
            cached = self.storage.get_cached_prefix(cache_prefix)
            if cached:
                return {"cached": True, "content": cached["cached_content"], "provider": cached.get("provider")}
        models = self._route_models(model)
        # estimate tokens and cost for bookkeeping
        est_tokens = self._estimate_tokens(messages)
        est_cost = self._estimate_cost(model, est_tokens)
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        last_err = None
        start = time.time()
        for m in models:
            payload = {"model": m, "messages": messages, "max_tokens": max_tokens}
            try:
                data = self._post("/v1/chat/completions", payload, headers)
                # optionally cache stable prefixes
                if cache_prefix:
                    # store content in cache for 1 hour
                    content = data.get("choices", [{}])[0].get("message", {}).get("content") if isinstance(data, dict) else str(data)
                    # store cache hints metadata per provider
                    self.storage.cache_prefix_with_hints(cache_prefix, provider=m, content=content, ttl_seconds=3600, cache_hints=cache_hints)
                # estimate cost placeholder
                duration = time.time() - start
                token_cost = est_cost
                if session_id:
                    # record token cost into session row (additive)
                    try:
                        self.storage.add_session_token_cost(session_id, token_cost, delta_tokens=est_tokens)
                    except Exception:
                        pass
                return {"cached": False, "data": data}
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"All model fallbacks failed: {last_err}")
