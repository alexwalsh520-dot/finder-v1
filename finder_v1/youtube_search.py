from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


def _request_json(url: str) -> Dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _score_candidate(profile: Dict[str, object], item: Dict) -> int:
    username = (profile.get("username") or "").lower()
    full_name = (profile.get("full_name") or "").lower()
    title = (item.get("snippet", {}).get("channelTitle") or "").lower()
    description = (item.get("snippet", {}).get("description") or "").lower()

    score = 0
    normalized_title = re.sub(r"[^a-z0-9]+", "", title)
    normalized_name = re.sub(r"[^a-z0-9]+", "", full_name)
    normalized_user = re.sub(r"[^a-z0-9]+", "", username)
    if normalized_name and normalized_name in normalized_title:
        score += 5
    if normalized_user and normalized_user in normalized_title:
        score += 5
    if username and username in description:
        score += 2
    if any(term in f"{title} {description}" for term in ("fitness", "bodybuilding", "gym", "workout", "coach", "physique")):
        score += 2
    return score


def search_channel(profile: Dict[str, object], youtube_api_key: str) -> Optional[str]:
    if not youtube_api_key:
        return None

    username = (profile.get("username") or "").strip()
    full_name = (profile.get("full_name") or "").strip()
    queries = []
    if full_name:
        queries.append(full_name)
        queries.append(f"{full_name} fitness")
        queries.append(f"{full_name} bodybuilding")
    if username:
        queries.append(username)
        queries.append(f"@{username}")

    best_url = None
    best_score = 0
    seen_queries = set()

    for query in queries:
        query = query.strip()
        if not query or query in seen_queries:
            continue
        seen_queries.add(query)
        params = urllib.parse.urlencode({
            "part": "snippet",
            "q": query,
            "type": "channel",
            "maxResults": 5,
            "key": youtube_api_key,
        })
        url = f"https://www.googleapis.com/youtube/v3/search?{params}"
        try:
            data = _request_json(url)
        except Exception:
            continue
        for item in data.get("items", []):
            score = _score_candidate(profile, item)
            if score <= best_score:
                continue
            channel_id = item.get("id", {}).get("channelId")
            if not channel_id:
                continue
            best_score = score
            best_url = f"https://www.youtube.com/channel/{channel_id}"

    if best_score >= 5:
        return best_url
    return None
