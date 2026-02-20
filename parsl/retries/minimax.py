from __future__ import annotations

import os
from typing import Any, Optional

from openai import OpenAI


class MiniMaxOpenAICompatClient:
    """OpenAI-compatible client for MiniMax (and any OpenAI-compatible API)."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: str = "https://api.minimax.io/v1",
        request_timeout_s: float = 60.0,
        client: Optional[Any] = None,
    ) -> None:
        resolved_key = api_key or os.environ.get("MINIMAX_API_KEY")
        if not resolved_key and client is None:
            raise ValueError("MINIMAX_API_KEY is required")

        self._timeout = request_timeout_s
        self._client: OpenAI = client or OpenAI(
            api_key=resolved_key,
            base_url=base_url.rstrip("/"),
        )

    def generate_patch(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> str:
        if temperature <= 0.0 or temperature > 1.0:
            raise ValueError("temperature must be in (0, 1]")

        response = self._client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            n=1,
            timeout=self._timeout,
        )

        content = response.choices[0].message.content
        if not isinstance(content, str):
            raise RuntimeError(f"MiniMax response content must be string, got {type(content)}")

        return content
