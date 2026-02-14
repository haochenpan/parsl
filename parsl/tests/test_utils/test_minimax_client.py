import pytest

from parsl.retries.minimax import MiniMaxOpenAICompatClient


class FakeResponse:
    def __init__(self, status_code, payload, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        return self.response


@pytest.mark.local
def test_minimax_client_openai_compatible_payload():
    response = FakeResponse(
        200,
        {
            "choices": [
                {
                    "message": {
                        "content": '{"patched_function_source":"def f():\\n    return 1"}'
                    }
                }
            ]
        },
    )
    session = FakeSession(response)

    client = MiniMaxOpenAICompatClient(
        api_key="unit-key",
        base_url="https://api.minimax.io/v1",
        session=session,
    )

    content = client.generate_patch(
        model="MiniMax-M2.5",
        system_prompt="system",
        user_prompt="user",
        temperature=0.2,
    )

    assert "patched_function_source" in content
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == "https://api.minimax.io/v1/chat/completions"
    assert call["headers"]["Authorization"] == "Bearer unit-key"
    assert call["json"]["model"] == "MiniMax-M2.5"
    assert call["json"]["n"] == 1


@pytest.mark.local
def test_minimax_client_non_200_raises():
    session = FakeSession(FakeResponse(401, {}, text="unauthorized"))
    client = MiniMaxOpenAICompatClient(api_key="unit-key", session=session)

    with pytest.raises(RuntimeError, match="status=401"):
        client.generate_patch(
            model="MiniMax-M2.5",
            system_prompt="system",
            user_prompt="user",
            temperature=0.2,
        )


@pytest.mark.local
def test_minimax_client_malformed_response_raises():
    session = FakeSession(FakeResponse(200, {"choices": []}))
    client = MiniMaxOpenAICompatClient(api_key="unit-key", session=session)

    with pytest.raises(RuntimeError, match="message.content"):
        client.generate_patch(
            model="MiniMax-M2.5",
            system_prompt="system",
            user_prompt="user",
            temperature=0.2,
        )


@pytest.mark.local
def test_minimax_client_temperature_validation():
    session = FakeSession(FakeResponse(200, {"choices": [{"message": {"content": "{}"}}]}))
    client = MiniMaxOpenAICompatClient(api_key="unit-key", session=session)

    with pytest.raises(ValueError, match="temperature"):
        client.generate_patch(
            model="MiniMax-M2.5",
            system_prompt="system",
            user_prompt="user",
            temperature=0.0,
        )
