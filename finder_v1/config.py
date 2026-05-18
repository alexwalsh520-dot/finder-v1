from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = STATE_DIR / "finder.sqlite3"
LOCK_PATH = STATE_DIR / "pipeline.lock"

SIMILAR_ACTOR = "thenetaji/instagram-related-user-scraper"
ENRICH_ACTOR = "apify/instagram-profile-scraper"
DOC_ACTOR = "dataovercoffee/youtube-channel-business-email-scraper"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

MIN_FOLLOWERS = 100_000

KNOWN_BRAND_DOMAINS = [
    "gymshark", "youngla", "darcsport", "1stphorm", "alphalete", "nvgtn",
    "ghostlifestyle", "rawgear", "gymreapers", "gorillawear", "musclenation",
    "buffbunny", "rawnutrition", "revivemd", "morphogen", "evogen", "myprotein",
    "scitec", "rpstrength", "marekhealth", "elev8", "transparentlabs",
    "celsius", "armra", "tlfapparel", "vqfit", "kontrolled", "biowell",
    "mutant", "iammutant", "prosupps", "dragonpharma", "boohooman",
    "fathersons", "axeandsledge", "hugesupplements", "megafitmeals",
    "bumenergy", "flexpromeals", "newtech", "momentus", "htltsupps",
    "cutlernutrition", "transparentlabs", "supplementworld", "iovate",
]

PLATFORM_DOMAINS = [
    "cameo.com", "calendly.com", "typeform.com", "google.com", "docs.google.com",
    "forms.gle", "linktr.ee", "beacons.ai", "stan.store", "mailchimp.com",
    "hoo.be", "lnk.bio", "bio.site", "link.me", "bit.ly", "komi.io",
]

SOCIAL_HOSTS = [
    "instagram.com", "youtube.com", "youtu.be", "tiktok.com", "twitter.com",
    "x.com", "facebook.com", "snapchat.com", "threads.net", "linkedin.com",
]

LEGAL_PATHS = [
    "/contact", "/about", "/work-with-me", "/media-kit", "/booking",
    "/terms", "/terms-and-conditions", "/privacy", "/privacy-policy",
    "/legal", "/impressum",
]

SUBPAGE_HINTS = [
    "contact", "about", "privacy", "terms", "legal", "impressum", "work",
    "booking", "media", "kit", "hire", "inquiry",
]

PERSON_HINTS = [
    "coach", "athlete", "trainer", "founder", "creator", "host", "dad", "mom",
    "girl", "guy", "ceo", "nutrition", "physique", "bodybuilding", "fitness",
    "bodybuilder", "ifbb", "pro", "champ", "champion", "olympia",
]

BRAND_REJECT_HINTS = [
    "official page", "shop now", "order now", "shipping worldwide", "store",
    "supplements", "apparel", "magazine", "news", "fan page", "tribute",
    "memorial", "brand", "company", "team account",
]

FITNESS_SIGNALS = [
    "fitness", "gym", "workout", "training", "coach", "coaching", "physique",
    "bodybuilding", "lifting", "strength", "hypertrophy", "protein", "macro",
    "prep", "athlete", "muscle", "recomp", "powerlifting", "wellness",
]

NON_ENGLISH_HINTS = [
    "espanol", "portugues", "рус", "العربية", "français", "deutsch", "italiano",
]

MANAGEMENT_HINTS = [
    "management", "mgmt", "booking", "bookings", "agent", "agency", "talent",
    "inquiries", "inquiry", "pr@", "publicist", "marketing", "media", "mkt",
    "partnership", "partnerships", "representation", "rep",
    "genflow", "vividalchemy",
]

JUNK_EMAIL_PATTERNS = [
    "example.com", "w3.org", "schema.org", "linktr.ee", "cloudflare",
    "noreply", "no-reply", "donotreply", "mailer-daemon", ".png", ".jpg",
    ".jpeg", ".gif", ".webp", ".svg", ".css", ".js", "support@", "hello@cameo",
    "name@gmail.com", "yourname@", "email@email", "test@", "example@",
    "sentry-next.wixpress.com", "ingest.sentry.io",
]

TRUE_VALUES = {"1", "true", "yes", "on"}


def ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_env() -> None:
    candidates = [
        Path.cwd() / ".env",
        Path.cwd() / ".env.local",
        BASE_DIR / ".env",
        Path.cwd() / ".env.production",
    ]
    for path in candidates:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def api_config() -> Dict[str, str]:
    load_env()
    return {
        "apify_token": os.environ.get("APIFY_API_TOKEN", ""),
        "dataovercoffee_api_key": os.environ.get("DATAOVERCOFFEE_API_KEY", ""),
        "anthropic_key": os.environ.get("ANTHROPIC_API_KEY", ""),
        "groq_key": os.environ.get("GROQ_API_KEY", ""),
        "youtube_key": os.environ.get("YOUTUBE_API_KEY", ""),
        "smartlead_api_key": os.environ.get("SMARTLEAD_API_KEY", ""),
        "supabase_url": os.environ.get("SUPABASE_URL", ""),
        "supabase_service_role_key": os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
        "finder_timezone": business_timezone_name(),
        "finder_output_bucket": os.environ.get("FINDER_OUTPUT_BUCKET", "").strip(),
    }


def business_timezone_name() -> str:
    load_env()
    return os.environ.get("FINDER_TIMEZONE", "Asia/Makassar").strip() or "Asia/Makassar"


def safe_json_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def flatten_text_parts(parts: List[str]) -> str:
    return " ".join(p.strip() for p in parts if p and p.strip())


def require_green_light_brand_in_bio() -> bool:
    load_env()
    return os.environ.get("REQUIRE_GREEN_LIGHT_BRAND_IN_BIO", "").strip().lower() in TRUE_VALUES


def green_light_brands_file() -> Path | None:
    load_env()
    raw = os.environ.get("GREEN_LIGHT_BRANDS_FILE", "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def _normalize_brand_token(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"^https?://(www\.)?(instagram\.com/)?", "", text)
    text = text.split("/", 1)[0]
    text = text.lstrip("@#")
    text = re.sub(r"[^a-z0-9._]", "", text)
    return text


def load_green_light_brands() -> List[str]:
    brands: List[str] = []
    path = green_light_brands_file()
    if path and path.exists() and path.is_file():
        for line in path.read_text(errors="ignore").splitlines():
            for cell in csv.reader([line]).__next__():
                normalized = _normalize_brand_token(cell)
                if normalized and normalized not in brands:
                    brands.append(normalized)
    if not brands:
        brands = [_normalize_brand_token(brand) for brand in KNOWN_BRAND_DOMAINS]
    return [brand for brand in brands if brand]
