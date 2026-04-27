from __future__ import annotations

import re
from typing import Any, Dict

from .config import NON_ENGLISH_HINTS


QUALIFICATION_VERSION = "simple_v1"

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


def _bio_language_text(profile: Dict[str, Any]) -> str:
    bio = str(profile.get("biography") or "").lower()
    bio = re.sub(r"https?://\S+|www\.\S+", " ", bio)
    bio = re.sub(r"@\S+", " ", bio)
    bio = re.sub(r"#\S+", " ", bio)
    return " ".join(bio.split())


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


def _obvious_non_creator_profile(profile: Dict[str, Any]) -> bool:
    username = (profile.get("username") or "").lower()
    full_name = (profile.get("full_name") or "").lower()
    bio = (profile.get("biography") or "").lower()
    category = (profile.get("business_category") or "").lower()
    scoped = " ".join([username, full_name, category, bio[:220]])
    hard_rejects = [
        "fan page",
        "tribute",
        "memorial",
        "magazine",
        "news",
        "media outlet",
        "official store",
        "shop now",
        "order now",
        "shipping worldwide",
        "customer support",
        "brand account",
        "apparel store",
        "supplement store",
    ]
    return any(hint in scoped for hint in hard_rejects)


def deterministic_qualification(profile: Dict[str, Any]) -> Dict[str, Any]:
    bio_language_text = _bio_language_text(profile)
    if _obvious_non_english(bio_language_text):
        return {
            "qualified": False,
            "reject_reason": "not_english",
            "confidence": 0.95,
            "why": "Bio contains obvious non-English language signals.",
            "needs_ai": False,
        }

    if _obvious_non_creator_profile(profile):
        return {
            "qualified": False,
            "reject_reason": "not_person",
            "confidence": 0.9,
            "why": "Profile looks like a fan, news, store, or brand account instead of a creator.",
            "needs_ai": False,
        }

    return {
        "qualified": True,
        "reject_reason": None,
        "confidence": 0.8,
        "why": "Profile passes the simple hard gates. Fitness relevance comes from the qualified seed relationship.",
        "needs_ai": False,
    }
