from __future__ import annotations

import html
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Iterable, List, Optional, Tuple

from .config import JUNK_EMAIL_PATTERNS, KNOWN_BRAND_DOMAINS, LEGAL_PATHS, MANAGEMENT_HINTS, PLATFORM_DOMAINS, SOCIAL_HOSTS, SUBPAGE_HINTS


REVIEWABLE_EMAIL_TYPES = {"personal", "management", "generic_business", "brand"}


EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
LINK_RE = re.compile(r"""href=["']([^"'#]+)["']""", re.I)
YOUTUBE_RE = re.compile(r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s\"'<>]+", re.I)


def extract_emails(text: str) -> List[str]:
    found = []
    for raw in EMAIL_RE.findall(text or ""):
        email = raw.lower()
        if any(pattern in email for pattern in JUNK_EMAIL_PATTERNS):
            continue
        if email not in found:
            found.append(email)
    return found


def fetch_text(url: str) -> Optional[str]:
    last_error: Exception | None = None
    for attempt in range(3):
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                content_type = response.headers.get("Content-Type", "")
                if "text" not in content_type and "html" not in content_type:
                    return None
                return response.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {408, 425, 429, 500, 502, 503, 504} or attempt == 2:
                return None
            time.sleep(1.0 * (attempt + 1))
        except Exception as exc:
            last_error = exc
            if attempt == 2:
                return None
            time.sleep(1.0 * (attempt + 1))
    if last_error:
        return None
    return None


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith("http"):
        url = f"https://{url}"
    return url


def url_kind(url: str) -> str:
    url = url.lower()
    host = urllib.parse.urlparse(url).hostname or ""
    if any(domain in host for domain in SOCIAL_HOSTS):
        return "social"
    if any(domain in host for domain in PLATFORM_DOMAINS):
        return "platform"
    if any(domain in host for domain in KNOWN_BRAND_DOMAINS):
        return "brand"
    if re.search(r"\.(png|jpg|jpeg|gif|webp|svg|css|js|woff2?)$", url):
        return "asset"
    return "personal"


def root_domain(host: str) -> str:
    host = (host or "").lower().strip(".")
    parts = [part for part in host.split(".") if part]
    if len(parts) <= 2:
        return host
    return ".".join(parts[-2:])


def email_host_kind(host: str) -> str:
    host = (host or "").lower()
    if not host:
        return "unknown"
    if any(pattern in host for pattern in JUNK_EMAIL_PATTERNS):
        return "junk"
    if any(token in host for token in ("sentry", "wixpress", "cloudflare", "schema.org", "w3.org")):
        return "junk"
    if any(domain in host for domain in PLATFORM_DOMAINS):
        return "platform"
    if any(domain in host for domain in KNOWN_BRAND_DOMAINS):
        return "brand"
    if any(token in host for token in ("agency", "talent", "management", "mgmt", "media", "marketing", "mkt")):
        return "management"
    return "unknown"


def source_matches_email_domain(email_host: str, source_url: str) -> bool:
    if not source_url:
        return False
    source_host = urllib.parse.urlparse(normalize_url(source_url)).hostname or ""
    if not source_host:
        return False
    return root_domain(source_host) == root_domain(email_host)


def classify_email(email: str, source_url: str) -> Tuple[str, str]:
    lower = email.lower()
    if "@" not in lower:
        return "junk", "Email is malformed."
    local_part, host = lower.split("@", 1)
    source_kind = url_kind(source_url) if source_url else ""
    host_kind = email_host_kind(host)
    placeholder_locals = {
        "name", "user", "you", "yourname", "email", "test", "example",
        "first", "firstname", "last", "lastname", "first.last", "john",
        "jane", "john.doe", "jane.doe",
    }
    placeholder_hosts = {"domain.com", "email.com"}
    generic_locals = {
        "cs", "ops", "office", "team", "admin", "info", "hello", "contact",
        "support", "sales", "customerservice", "privacy", "legal", "copyright",
        "terms", "gdpr", "creators", "ecommerce", "anfragen",
    }
    management_locals = {"booking", "bookings", "management", "mgmt", "agent", "agents", "inquiries", "inquiry", "partnerships", "partnership"}

    if local_part in placeholder_locals or host in placeholder_hosts:
        return "junk", "Email looks like a placeholder instead of a real inbox."
    if local_part == "name" and host == "mail.com":
        return "junk", "Email looks like a placeholder instead of a real inbox."
    if host_kind == "junk":
        return "junk", "Email looks like a monitoring, platform artifact, or other scraping garbage."
    if host_kind == "platform":
        return "platform", "Email belongs to a platform instead of the creator."
    if host_kind == "brand":
        return "brand", "Email domain belongs to a sponsor or brand site."
    if local_part in management_locals and host_kind == "management":
        return "management", "Email looks like a real booking or talent-management address."
    if any(hint in lower for hint in MANAGEMENT_HINTS):
        return "management", "Email looks like a management or booking address."
    if host_kind == "management":
        return "management", "Email domain looks like an agency, media, or management service."
    if source_kind == "platform":
        return "platform", "Email came from a platform page instead of a creator-owned site."
    if local_part in generic_locals:
        if source_matches_email_domain(host, source_url):
            return "generic_business", "Email is a creator-site generic inbox, not a direct personal address."
        return "generic_business", "Email is a generic business inbox instead of a direct creator or management contact."
    if local_part.startswith("preview."):
        return "generic_business", "Email looks like a shared preview or early-access inbox."
    return "personal", "Email appears to be a direct or creator-owned address."


def is_reviewable_email_type(email_type: str | None) -> bool:
    return (email_type or "").strip().lower() in REVIEWABLE_EMAIL_TYPES


def extract_links(html_text: str, base_url: str) -> List[str]:
    links = []
    for raw in LINK_RE.findall(html_text or ""):
        resolved = urllib.parse.urljoin(base_url, html.unescape(raw))
        if resolved not in links:
            links.append(resolved)
    return links


def extract_youtube_urls(text: str) -> List[str]:
    found = []
    for match in YOUTUBE_RE.findall(text or ""):
        if match not in found:
            found.append(match)
    return found


def candidate_subpages(html_text: str, base_url: str) -> List[str]:
    host = urllib.parse.urlparse(base_url).hostname
    discovered = []
    for link in extract_links(html_text, base_url):
        parsed = urllib.parse.urlparse(link)
        if parsed.hostname != host:
            continue
        text = parsed.path.lower()
        if any(hint in text for hint in SUBPAGE_HINTS) and link not in discovered:
            discovered.append(link)
    for suffix in LEGAL_PATHS:
        link = urllib.parse.urljoin(base_url, suffix)
        if link not in discovered:
            discovered.append(link)
    return discovered[:12]


def collect_candidate_sites(html_text: str, base_url: str) -> List[str]:
    candidates = []
    for link in extract_links(html_text, base_url):
        kind = url_kind(link)
        if kind != "personal":
            continue
        host = urllib.parse.urlparse(link).hostname
        if not host:
            continue
        if link not in candidates:
            candidates.append(link)
    return candidates[:4]


def search_page(url: str, source_method: str, path_label: str, html_text: Optional[str] = None) -> List[Dict[str, str]]:
    html_text = html_text if html_text is not None else fetch_text(url)
    if not html_text:
        return []
    results = []
    mailto_matches = re.findall(r"mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})", html_text, flags=re.I)
    for email in mailto_matches + extract_emails(html_text):
        email_type, notes = classify_email(email, url)
        results.append({
            "email": email,
            "email_type": email_type,
            "source_method": source_method,
            "source_url": url,
            "source_path": path_label,
            "notes": notes,
        })
    unique = []
    seen = set()
    for row in results:
        if row["email"] in seen:
            continue
        seen.add(row["email"])
        unique.append(row)
    return unique


def deep_email_search(profile: Dict[str, object]) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    bio_text = " ".join([
        str(profile.get("biography", "") or ""),
        str(profile.get("external_url", "") or ""),
    ])
    for email in extract_emails(bio_text):
        email_type, notes = classify_email(email, "bio")
        candidates.append({
            "email": email,
            "email_type": email_type,
            "source_method": "bio",
            "source_url": "",
            "source_path": "bio",
            "notes": notes,
        })

    start_urls = []
    external_url = normalize_url(str(profile.get("external_url", "") or ""))
    if external_url:
        start_urls.append(("external", external_url))

    for origin, start_url in start_urls:
        start_html = fetch_text(start_url)
        if not start_html:
            continue
        page_kind = url_kind(start_url)
        source_method = "link_hub" if page_kind == "platform" else "site"
        candidates.extend(search_page(start_url, source_method, "home", start_html))

        if page_kind == "platform":
            for linked_site in collect_candidate_sites(start_html, start_url):
                linked_html = fetch_text(linked_site) or ""
                candidates.extend(search_page(linked_site, "linked_site", "home", linked_html))
                subpages = candidate_subpages(linked_html, linked_site)
                for subpage in subpages:
                    candidates.extend(search_page(subpage, "linked_site_subpage", urllib.parse.urlparse(subpage).path or "/"))
        elif page_kind == "personal":
            for subpage in candidate_subpages(start_html, start_url):
                candidates.extend(search_page(subpage, "site_subpage", urllib.parse.urlparse(subpage).path or "/"))

    unique = []
    seen = set()
    for row in candidates:
        key = (row["email"], row["source_method"], row["source_path"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def discover_youtube_urls(profile: Dict[str, object]) -> List[str]:
    found: List[str] = []
    sources = [
        str(profile.get("biography", "") or ""),
        str(profile.get("external_url", "") or ""),
        " ".join(profile.get("recent_captions", []) or []),
    ]
    for source in sources:
        for url in extract_youtube_urls(source):
            if url not in found:
                found.append(url)

    external_url = normalize_url(str(profile.get("external_url", "") or ""))
    if not external_url:
        return found

    start_html = fetch_text(external_url)
    if not start_html:
        return found

    for url in extract_youtube_urls(start_html):
        if url not in found:
            found.append(url)

    page_kind = url_kind(external_url)
    if page_kind == "platform":
        for linked_site in collect_candidate_sites(start_html, external_url):
            page_html = fetch_text(linked_site) or ""
            for url in extract_youtube_urls(page_html):
                if url not in found:
                    found.append(url)
    return found


def best_candidate(candidates: Iterable[Dict[str, str]]) -> Optional[Dict[str, str]]:
    ranking = {
        "personal": 0,
        "management": 1,
        "generic_business": 2,
        "platform": 3,
        "brand": 4,
        "junk": 5,
    }
    filtered = [c for c in candidates if is_reviewable_email_type(c.get("email_type"))]
    if not filtered:
        return None
    return sorted(filtered, key=lambda c: (ranking.get(c["email_type"], 9), c["source_method"], c["source_path"]))[0]
