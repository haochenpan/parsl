from __future__ import annotations

from typing import Protocol


class LLMRetryClient(Protocol):
    """Protocol for LLM backends used by retry_llm_policy."""

    def generate_patch(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> str:
        """Return assistant content for patch generation."""
