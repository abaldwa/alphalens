"""
systems/copilot/llm_client.py

Thin OpenRouter chat-completions wrapper — the first LLM integration in
this codebase. Raises on any failure (missing key, HTTP error, malformed
response); NEVER falls back to a canned/synthetic strategy spec. A mocked
or offline "looks-plausible" LLM response would be exactly the kind of
fabricated stand-in Absolute Rule 6 forbids, since its output becomes a
strategy a user might backtest and save.
"""

import json
import logging
from typing import Any, Dict

import requests

from config.settings import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    OPENROUTER_MODEL,
    OPENROUTER_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


class LLMConfigError(RuntimeError):
    """Raised when OPENROUTER_API_KEY is not configured."""


class LLMCallError(RuntimeError):
    """Raised when the OpenRouter call fails or returns an unusable response."""


def call_openrouter_json(system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    """Call OpenRouter chat completions, forcing JSON-object output.

    Returns the parsed JSON dict from the model's message content.
    Raises LLMConfigError / LLMCallError on any failure rather than
    returning a default or substitute result.
    """
    if not OPENROUTER_API_KEY:
        raise LLMConfigError(
            "OPENROUTER_API_KEY is not set. Co-Pilot requires a real OpenRouter "
            "API key (see BuildLog.md's Co-Pilot section) — there is no offline "
            "fallback mode and no default value substituted here."
        )

    try:
        response = requests.post(
            f"{OPENROUTER_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.0,
            },
            timeout=OPENROUTER_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise LLMCallError(f"OpenRouter request failed: {exc}") from exc

    payload = response.json()
    try:
        content = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise LLMCallError(f"OpenRouter response missing expected fields: {payload}") from exc

    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        raise LLMCallError(f"OpenRouter returned non-JSON content: {content}") from exc
