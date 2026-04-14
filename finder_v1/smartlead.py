from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List


BASE = "https://server.smartlead.ai/api/v1"
USER_AGENT = "finder-v1-worker/1.0"
DEFAULT_PAGE_SIZE = 100


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


def list_campaigns(api_key: str) -> List[Dict[str, Any]]:
    if not api_key:
        return []
    payload = smartlead_get_json(api_key, "/campaigns/", {})
    return payload if isinstance(payload, list) else []


def list_campaign_leads(api_key: str, campaign_id: str | int, *, limit: int = DEFAULT_PAGE_SIZE, offset: int = 0) -> Dict[str, Any]:
    if not api_key:
        return {"total_leads": "0", "data": [], "offset": offset, "limit": limit}
    payload = smartlead_get_json(
        api_key,
        f"/campaigns/{campaign_id}/leads",
        {
            "limit": limit,
            "offset": offset,
        },
    )
    return payload if isinstance(payload, dict) else {"total_leads": "0", "data": [], "offset": offset, "limit": limit}


def normalize_instagram_handle(value: str | None) -> str:
    cleaned = (value or "").strip().lower()
    if not cleaned:
        return ""
    if "instagram.com/" in cleaned:
        cleaned = cleaned.split("instagram.com/", 1)[1]
    cleaned = cleaned.replace("@", "").strip("/")
    if "/" in cleaned:
        cleaned = cleaned.split("/", 1)[0]
    if "?" in cleaned:
        cleaned = cleaned.split("?", 1)[0]
    return cleaned


def extract_instagram_handles(lead_payload: Dict[str, Any]) -> List[str]:
    custom_fields = lead_payload.get("custom_fields") or {}
    candidates = [
        custom_fields.get("instagram_handle"),
        custom_fields.get("instagram_username"),
        custom_fields.get("instagram_url"),
        custom_fields.get("instagram"),
    ]
    handles: List[str] = []
    for raw in candidates:
        handle = normalize_instagram_handle(str(raw) if raw is not None else "")
        if handle and handle not in handles:
            handles.append(handle)
    return handles


def build_account_lead_index(api_key: str) -> Dict[str, Any]:
    campaigns = list_campaigns(api_key)
    emails: Dict[str, Dict[str, Any]] = {}
    handles: Dict[str, Dict[str, Any]] = {}
    total_campaigns = 0
    total_leads = 0

    for campaign in campaigns:
        campaign_id = str(campaign.get("id") or "").strip()
        if not campaign_id:
            continue
        total_campaigns += 1
        offset = 0
        while True:
            page = list_campaign_leads(api_key, campaign_id, limit=DEFAULT_PAGE_SIZE, offset=offset)
            rows = page.get("data") or []
            if not isinstance(rows, list) or not rows:
                break
            for row in rows:
                lead = row.get("lead") or {}
                lead_id = str(lead.get("id") or "").strip()
                email = (lead.get("email") or "").strip().lower()
                membership = {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign.get("name") or "",
                    "campaign_status": campaign.get("status") or "",
                    "lead_id": lead_id,
                    "lead_status": row.get("status") or "",
                }
                if email:
                    entry = emails.setdefault(email, {"campaign_ids": set(), "memberships": []})
                    entry["campaign_ids"].add(campaign_id)
                    entry["memberships"].append(membership)
                for handle in extract_instagram_handles(lead):
                    entry = handles.setdefault(handle, {"campaign_ids": set(), "memberships": []})
                    entry["campaign_ids"].add(campaign_id)
                    entry["memberships"].append(membership)
                total_leads += 1
            page_limit = int(page.get("limit") or len(rows) or DEFAULT_PAGE_SIZE)
            total = int(page.get("total_leads") or 0)
            offset = int(page.get("offset") or offset) + page_limit
            if not total or offset >= total:
                break

    return {
        "campaigns": campaigns,
        "campaign_count": total_campaigns,
        "lead_count": total_leads,
        "emails": emails,
        "handles": handles,
    }


def extract_campaign_id(payload: Dict[str, Any]) -> str:
    memberships = payload.get("lead_campaign_data") or []
    if memberships:
        campaign_id = memberships[0].get("campaign_id")
        if campaign_id is not None:
            return str(campaign_id)
    members = payload.get("memberships") or []
    if members:
        campaign_id = members[0].get("campaign_id")
        if campaign_id is not None:
            return str(campaign_id)
    campaign_id = payload.get("campaign_id")
    return str(campaign_id) if campaign_id is not None else ""


def is_rate_limited_error(exc: Exception) -> bool:
    return isinstance(exc, urllib.error.HTTPError) and exc.code == 429
