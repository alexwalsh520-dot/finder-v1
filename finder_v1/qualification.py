from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from .config import (
    ANTHROPIC_MODEL,
    BRAND_REJECT_HINTS,
    FITNESS_SIGNALS,
    KNOWN_BRAND_DOMAINS,
    load_green_light_brands,
    NON_ENGLISH_HINTS,
    PERSON_HINTS,
    require_green_light_brand_in_bio,
)


QUALIFICATION_VERSION = "v3"

NON_LATIN_RANGES = [
    (0x0370, 0x03FF),  # Greek
    (0x0400, 0x04FF),  # Cyrillic
    (0x0590, 0x05FF),  # Hebrew
    (0x0600, 0x06FF),  # Arabic
    (0x0750, 0x077F),  # Arabic Supplement
    (0x0900, 0x097F),  # Devanagari
    (0x0980, 0x09FF),  # Bengali
    (0x0E00, 0x0E7F),  # Thai
    (0x3040, 0x309F),  # Hiragana
    (0x30A0, 0x30FF),  # Katakana
    (0x3400, 0x4DBF),  # CJK Extension A
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs
    (0xAC00, 0xD7AF),  # Hangul
]

NON_ENGLISH_WORD_PATTERNS = [
    r"\bespañol\b",
    r"\bportugu[eê]s\b",
    r"\bfran[cç]ais\b",
    r"\bdeutsch\b",
    r"\bitaliano\b",
    r"\batleta\b",
    r"\bentren(a|ador|adora|amiento)\b",
    r"\bnutri[cç][aã]o\b",
    r"\bsuplementos\b",
    r"\bparceri(as|a)\b",
    r"\bcontato\b",
    r"\bcorreo\b",
    r"\bs[íi]gueme\b",
    r"\bbienvenid[oa]s?\b",
    r"\btrein(o|ador|adora)\b",
    r"\bsa[uú]de\b",
    r"\bfor[aã]\b",
    r"\bconstruyendo\b",
    r"\bdisciplina y fe\b",
    r"\bganhar massa\b",
    r"\btransforma[cç][aã]o\b",
    r"\bperfil oficial\b",
    r"\bcoach en l[ií]nea\b",
    r"\basesor[ií]as\b",
    r"\bhablamos\b",
    r"\benlaces?\b",
    r"\bprenotazioni\b",
    r"\bcontatti\b",
    r"\ballenamento\b",
    r"\boffizielle[rsn]?\b",
    r"\bzusammenarbeit\b",
]


def _text_blob(profile: Dict[str, Any]) -> str:
    parts = [
        profile.get("username", ""),
        profile.get("full_name", ""),
        profile.get("biography", ""),
        profile.get("business_category", ""),
        " ".join(profile.get("recent_captions", [])[:5]),
        profile.get("external_url", ""),
    ]
    return " ".join(str(p or "") for p in parts).lower()


def _content_language_text(profile: Dict[str, Any]) -> str:
    parts = [
        profile.get("biography", ""),
        " ".join(profile.get("recent_captions", [])[:5]),
    ]
    return " ".join(str(p or "") for p in parts).lower()


def _fitness_score(text: str) -> int:
    score = 0
    for signal in FITNESS_SIGNALS + KNOWN_BRAND_DOMAINS:
        if signal in text:
            score += 1
    return score


def _normalize_brand_token(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"^https?://(www\.)?(instagram\.com/)?", "", text)
    text = text.split("/", 1)[0]
    text = text.lstrip("@#")
    text = re.sub(r"[^a-z0-9._]", "", text)
    return text


def _bio_brand_matches(profile: Dict[str, Any]) -> List[str]:
    bio = (profile.get("biography") or "").lower()
    mentions = {
        _normalize_brand_token(match)
        for match in re.findall(r"@([a-z0-9._]+)", bio, flags=re.I)
    }
    matches = []
    for brand in load_green_light_brands():
        normalized = _normalize_brand_token(brand)
        if normalized and normalized in mentions and normalized not in matches:
            matches.append(normalized)
    return matches


def _obvious_not_person(profile: Dict[str, Any], text: str) -> bool:
    username = (profile.get("username") or "").lower()
    full_name = (profile.get("full_name") or "").lower()
    bio = (profile.get("biography") or "").lower()
    category = (profile.get("business_category") or "").lower()
    scoped = " ".join([username, full_name, category, bio[:180]])
    explicit_non_person = [
        "fan page", "memorial", "tribute", "magazine", "news", "official page",
        "shop now", "shipping worldwide", "customer support", "store", "supplements",
        "apparel", "brand account",
    ]
    return any(hint in scoped for hint in explicit_non_person)


def _brand_handle_not_person(profile: Dict[str, Any], text: str) -> bool:
    username = (profile.get("username") or "").lower()
    full_name = (profile.get("full_name") or "").lower()

    org_markers = ["llc", "official", "apparel", "store", "brand", "team", "magazine", "news"]
    if any(marker in username for marker in org_markers):
        return True

    for brand in KNOWN_BRAND_DOMAINS:
        if brand in username or brand in full_name:
            return True
    return False


def _contains_non_latin_script(text: str) -> bool:
    for ch in text:
        codepoint = ord(ch)
        for start, end in NON_LATIN_RANGES:
            if start <= codepoint <= end:
                return True
    return False


def _non_english_latin_pattern_hits(text: str) -> int:
    hits = 0
    for hint in NON_ENGLISH_HINTS:
        if hint in text:
            hits += 1
    for pattern in NON_ENGLISH_WORD_PATTERNS:
        if re.search(pattern, text, flags=re.I):
            hits += 1
    return hits


def _obvious_non_english(text: str) -> bool:
    if _contains_non_latin_script(text):
        return True
    return _non_english_latin_pattern_hits(text) > 0


def _likely_person(profile: Dict[str, Any]) -> bool:
    full_name = (profile.get("full_name") or "").strip()
    category = (profile.get("business_category") or "").lower()
    bio = (profile.get("biography") or "").lower()
    words = [w for w in re.split(r"[^A-Za-z]+", full_name) if w]
    if len(words) >= 2:
        return True
    if any(hint in category for hint in ("athlete", "trainer", "public figure", "coach")):
        return True
    if any(hint in bio for hint in ("ifbb", "bodybuilder", "athlete", "coach", "olympia")):
        return True
    return False


def deterministic_qualification(profile: Dict[str, Any]) -> Dict[str, Any]:
    text = _text_blob(profile)
    language_text = _content_language_text(profile)
    bio = (profile.get("biography") or "").lower()
    category = (profile.get("business_category") or "").lower()
    fitness_score = _fitness_score(text)
    brand_matches = _bio_brand_matches(profile)

    if _brand_handle_not_person(profile, text):
        return {
            "qualified": False,
            "reject_reason": "not_person",
            "confidence": 0.95,
            "why": "Username or profile identity matches a brand or organization, not a person.",
            "needs_ai": False,
        }

    if _obvious_not_person(profile, text) and not _likely_person(profile) and not any(hint in text for hint in PERSON_HINTS):
        return {
            "qualified": False,
            "reject_reason": "not_person",
            "confidence": 0.9,
            "why": "Profile looks like a brand, organization, or non-person account.",
            "needs_ai": False,
        }

    if require_green_light_brand_in_bio() and not brand_matches:
        return {
            "qualified": False,
            "reject_reason": "missing_brand_signal",
            "confidence": 0.99,
            "why": "Bio does not include any approved green-light brand username.",
            "needs_ai": False,
        }

    if _obvious_non_english(language_text):
        return {
            "qualified": False,
            "reject_reason": "not_english",
            "confidence": 0.95,
            "why": "Bio or recent captions include non-English language signals.",
            "needs_ai": False,
        }

    if fitness_score >= 2:
        return {
            "qualified": True,
            "reject_reason": None,
            "confidence": 0.75,
            "why": "Multiple fitness signals were found across the profile and captions.",
            "needs_ai": False,
        }

    if fitness_score == 0 and category and "fitness" not in category and "coach" not in category:
        return {
            "qualified": False,
            "reject_reason": "not_fitness",
            "confidence": 0.65,
            "why": "No meaningful fitness signal was found in the available profile context.",
            "needs_ai": True,
        }

    return {
        "qualified": True,
        "reject_reason": None,
        "confidence": 0.55,
        "why": "Profile is borderline and should go through reasoning before rejection.",
        "needs_ai": True,
    }


def build_prompt(profile: Dict[str, Any]) -> str:
    captions = "\n".join(f"- {caption}" for caption in profile.get("recent_captions", [])[:5])
    brand_matches = _bio_brand_matches(profile)
    brand_requirement = ""
    if require_green_light_brand_in_bio():
        if brand_matches:
            brand_requirement = f"4. missing_brand_signal = bio does not contain an approved sponsored brand username. Detected approved brand usernames: {', '.join(brand_matches)}\n"
        else:
            brand_requirement = "4. missing_brand_signal = bio does not contain an approved sponsored brand username\n"
    return f"""You are qualifying whether an Instagram profile is a valid fitness creator lead.

Reject only for one of these reasons:
1. not_person = brand, magazine, fan page, memorial, or other non-creator account
2. not_fitness = zero fitness signal across display name, bio, category, brand affiliations, and captions
3. not_english = any non-English language signal is present anywhere in the visible profile text
{brand_requirement}

If any non-English language signal is present, reject with not_english even if the profile is otherwise strong.
If the profile is a person and there is any meaningful fitness signal, prefer qualified=true only when rule 3 does not apply.

Return ONLY JSON with:
{{
  "qualified": true or false,
  "reject_reason": "not_person" | "not_fitness" | "not_english" | "missing_brand_signal" | null,
  "confidence": 0.0-1.0,
  "why": "one sentence"
}}

PROFILE
Display name: {profile.get("full_name", "")}
Username: @{profile.get("username", "")}
Bio: {profile.get("biography", "")}
Category: {profile.get("business_category", "")}
Website: {profile.get("external_url", "")}
Recent captions:
{captions or "- none"}
"""


def ai_qualification(profile: Dict[str, Any], anthropic_client: Any) -> Dict[str, Any]:
    prompt = build_prompt(profile)
    response_text = anthropic_client.classify(ANTHROPIC_MODEL, prompt)
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", response_text, flags=re.S)
        if not match:
            raise RuntimeError("Anthropic qualification response was not valid JSON")
        return json.loads(match.group(0))
