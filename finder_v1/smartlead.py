from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict


BASE = "https://server.smartlead.ai/api/v1"
USER_AGENT = "finder-v1-worker/1.0"


def smartlead_get_json(api_key: str, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    query = urllib.parse.urlencode({"api_key": api_key, **params})
    url = f"{BASE}{path}?{query}"
    last_error: Exception | None = None
    for attempt in range(3):
        req = urllib.request.Request(
            url,
            headers={
                "accept": "application/json",
                "user-agent": USER_AGENT,
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code == 429 and attempt < 2:
                time.sleep(15 * (attempt + 1))
                continue
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
        except Exception as exc:
            last_error = exc
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def lookup_smartlead_email(api_key: str, email: str) -> Dict[str, Any]:
    if not api_key or not email:
        return {}
    return smartlead_get_json(api_key, "/leads/", {"email": email})


def extract_campaign_id(payload: Dict[str, Any]) -> str:
    memberships = payload.get("lead_campaign_data") or []
    if memberships:
        campaign_id = memberships[0].get("campaign_id")
        if campaign_id is not None:
            return str(campaign_id)
    campaign_id = payload.get("campaign_id")
    return str(campaign_id) if campaign_id is not None else ""


def is_rate_limited_error(exc: Exception) -> bool:
    return isinstance(exc, urllib.error.HTTPError) and exc.code == 429
