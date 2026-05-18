from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Dict, List


def _should_retry(exc: Exception) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 429 or 500 <= exc.code < 600
    if isinstance(exc, urllib.error.URLError):
        return True
    return isinstance(exc, TimeoutError)


class ApifyHTTPClient:
    def __init__(self, token: str, dataovercoffee_api_key: str = ""):
        self.token = token
        self.dataovercoffee_api_key = dataovercoffee_api_key

    def enabled(self) -> bool:
        return bool(self.token)

    def _request(self, method: str, url: str, payload: Dict[str, Any] | None = None, timeout: int = 90) -> Dict[str, Any]:
        data = None
        headers = {"Content-Type": "application/json"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        last_error: Exception | None = None
        for attempt in range(3):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as exc:
                last_error = exc
                if attempt == 2 or not _should_retry(exc):
                    raise
                time.sleep(1.5 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def call_actor(self, actor_id: str, payload: Dict[str, Any], wait_secs: int = 120) -> Dict[str, Any]:
        if not self.token:
            raise RuntimeError("APIFY_API_TOKEN is not configured")
        actor = urllib.parse.quote(actor_id, safe="")
        params = urllib.parse.urlencode({"token": self.token, "waitForFinish": wait_secs})
        url = f"https://api.apify.com/v2/acts/{actor}/runs?{params}"
        data = self._request("POST", url, payload)
        return data["data"]

    def get_dataset_items(self, dataset_id: str) -> List[Dict[str, Any]]:
        params = urllib.parse.urlencode({"token": self.token, "clean": "true", "format": "json"})
        url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?{params}"
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=90) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as exc:
                last_error = exc
                if attempt == 2 or not _should_retry(exc):
                    raise
                time.sleep(1.5 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def get_run(self, run_id: str) -> Dict[str, Any]:
        params = urllib.parse.urlencode({"token": self.token})
        url = f"https://api.apify.com/v2/actor-runs/{run_id}?{params}"
        data = self._request("GET", url)
        return data["data"]

    def resurrect_run(self, run_id: str, wait_secs: int = 30) -> Dict[str, Any]:
        params = urllib.parse.urlencode({"token": self.token, "waitForFinish": wait_secs})
        url = f"https://api.apify.com/v2/actor-runs/{run_id}/resurrect?{params}"
        data = self._request("POST", url, {})
        return data["data"]

    def get_doc_run_status(self, run_id: str) -> Dict[str, Any]:
        params = urllib.parse.urlencode({"run_id": run_id})
        url = f"https://api.dataovercoffee.com/youtube/run-status?{params}"
        last_error: Exception | None = None
        raw = ""
        headers = {"User-Agent": "finder-v1/1.0"}
        if self.dataovercoffee_api_key:
            headers["Authorization"] = f"Bearer {self.dataovercoffee_api_key}"
            headers["x-api-key"] = self.dataovercoffee_api_key
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=60) as response:
                    raw = response.read().decode("utf-8")
                break
            except urllib.error.HTTPError as exc:
                if exc.code == 403:
                    return {"status": "forbidden", "_http_status": 403}
                last_error = exc
                if attempt == 2 or not _should_retry(exc):
                    raise
                time.sleep(1.5 * (attempt + 1))
            except Exception as exc:
                last_error = exc
                if attempt == 2 or not _should_retry(exc):
                    raise
                time.sleep(1.5 * (attempt + 1))
        if last_error and not raw:
            raise last_error
        return json.loads(raw) if raw else {}


class AnthropicHTTPClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def enabled(self) -> bool:
        return bool(self.api_key)

    def classify(self, model: str, prompt: str, max_tokens: int = 220) -> str:
        if not self.api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not configured")
        body = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01",
                "x-api-key": self.api_key,
            },
            method="POST",
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=90) as response:
                    data = json.loads(response.read().decode("utf-8"))
                break
            except Exception as exc:
                last_error = exc
                if attempt == 2 or not _should_retry(exc):
                    raise
                time.sleep(1.5 * (attempt + 1))
        else:
            assert last_error is not None
            raise last_error
        parts = data.get("content", [])
        return "".join(part.get("text", "") for part in parts)


class GroqHTTPClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def enabled(self) -> bool:
        return bool(self.api_key)

    def _request_transcription(self, fields: List[tuple[str, str]], file_part: tuple[str, bytes, str] | None = None) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("GROQ_API_KEY is not configured")

        boundary = f"----GroqBoundary{uuid.uuid4().hex}"
        parts: List[bytes] = []

        def add_field(name: str, value: str) -> None:
            parts.append(
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                    f"{value}\r\n"
                ).encode("utf-8")
            )

        for name, value in fields:
            add_field(name, value)

        if file_part is not None:
            filename, media_bytes, mime_type = file_part
            parts.append(
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
                    f"Content-Type: {mime_type}\r\n\r\n"
                ).encode("utf-8")
            )
            parts.append(media_bytes)

        parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
        body = b"".join(parts)

        req = urllib.request.Request(
            "https://api.groq.com/openai/v1/audio/transcriptions",
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "User-Agent": "finder-v1/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Groq transcription failed ({exc.code}): {body}") from exc

    def transcribe(
        self,
        media_bytes: bytes,
        filename: str,
        mime_type: str = "video/mp4",
        media_url: str | None = None,
        prompt: str | None = None,
    ) -> Dict[str, Any]:
        base_fields: List[tuple[str, str]] = [
            ("model", "whisper-large-v3-turbo"),
            ("response_format", "verbose_json"),
            ("temperature", "0"),
            ("language", "en"),
            ("timestamp_granularities[]", "segment"),
            ("timestamp_granularities[]", "word"),
        ]
        if prompt:
            base_fields.append(("prompt", prompt[:224]))

        if media_url:
            try:
                return self._request_transcription(base_fields + [("url", media_url)])
            except RuntimeError:
                pass

        return self._request_transcription(base_fields, file_part=(filename, media_bytes, mime_type))
