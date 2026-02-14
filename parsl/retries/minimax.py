from __future__ import annotations

import os
from typing import Any, Mapping, MutableMapping, Optional

import requests


class MiniMaxOpenAICompatClient:
    """OpenAI-compatible text client for MiniMax chat completions."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: str = "https://api.minimax.io/v1",
        request_timeout_s: float = 60.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("MINIMAX_API_KEY")
        if not self.api_key:
            raise ValueError("MINIMAX_API_KEY is required")

        self.base_url = base_url.rstrip("/")
        self.request_timeout_s = request_timeout_s
        self.session = session or requests.Session()

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

        url = f"{self.base_url}/chat/completions"
        headers: MutableMapping[str, str] = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: Mapping[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "n": 1,
        }

        response = self.session.post(url, headers=headers, json=payload, timeout=self.request_timeout_s)
        if response.status_code != 200:
            body = response.text[:1000]
            raise RuntimeError(f"MiniMax request failed with status={response.status_code}: {body}")

        try:
            data = response.json()
        except Exception as exc:
            raise RuntimeError("MiniMax response was not valid JSON") from exc

        try:
            content = data["choices"][0]["message"]["content"]
        except Exception as exc:
            raise RuntimeError("MiniMax response missing choices[0].message.content") from exc

        if not isinstance(content, str):
            raise RuntimeError(f"MiniMax response content must be string, got {type(content)}")

        return content
