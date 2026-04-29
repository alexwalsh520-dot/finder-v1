from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .apify_client import ApifyHTTPClient, GroqHTTPClient
from .config import BASE_DIR, DB_PATH, DOC_ACTOR, ENRICH_ACTOR, LOCK_PATH, MIN_FOLLOWERS, OUTPUT_DIR, SIMILAR_ACTOR, api_config, ensure_dirs
from .db import FinderDB
from .email_search import best_candidate, classify_email, deep_email_search, discover_youtube_urls, is_reviewable_email_type
from .qualification import QUALIFICATION_VERSION, deterministic_qualification
from .reel_transcriber import download_media, profile_reel_posts, transcript_output_paths, write_json, write_simple_pdf
from .runtime_lock import PipelineBusyError, pipeline_lock, read_lock_metadata
from .smartlead import build_account_lead_index, extract_campaign_id, normalize_instagram_handle
from .supabase_client import SupabaseClient
from .time_utils import business_now, business_tz, next_business_time_iso, next_interval_iso, today_business_date, utc_now_iso
from .youtube_search import search_channel

DEFAULT_DAILY_EMAIL_TARGET = 150
DEFAULT_QUALIFIED_TARGET = 100
DEFAULT_DAILY_BATCH_SIZE = 12
DEFAULT_DOC_CHECK_LIMIT = 100
DEFAULT_DOC_CHECK_TIME_BUDGET_SECONDS = 300
DEFAULT_DOC_PREFLIGHT_LIMIT = 500
DEFAULT_DOC_PREFLIGHT_TIME_BUDGET_SECONDS = 600
DEFAULT_SMARTLEAD_RECONCILE_LIMIT = 5000
ZERO_PROGRESS_ROTATION_THRESHOLD = 2
STALL_PROGRESS_MINUTES = 90
DOC_FORBIDDEN_TERMINAL_AFTER_HOURS = 2
LOW_YIELD_CYCLE_GAIN_THRESHOLD = 1
LOW_YIELD_STALL_THRESHOLD = 4
WORKER_COMPONENT = "finder_v1_worker"
CRON_JOB_DAILY_ID = "finder-v1-daily-run"
CRON_JOB_DOC_ID = "finder-v1-doc-harvest"
CRON_JOB_SMARTLEAD_ID = "finder-v1-smartlead-reconcile"
DAILY_RUN_SCHEDULE = "Daily 02:00 Bali time"
DOC_HARVEST_SCHEDULE = "Every 30 minutes"
SMARTLEAD_RECONCILE_SCHEDULE = "Every 2 minutes"
DAILY_RUN_LOCK_RETRY_SECONDS = 90
DAILY_RUN_LOCK_RETRY_INTERVAL_SECONDS = 5
TOP_UP_APP_SETTING_KEY = "finder_review_top_up_request"
RUNTIME_STATUS_APP_SETTING_KEY = "finder_v1_runtime_status"
TOP_UP_MINIMUM_EMAIL_BATCH = 25
TOP_UP_DEFAULT_MAX_CYCLES = 25
TOP_UP_DEFAULT_HARD_STOP_HOUR = 22
WORKER_EVENT_SAMPLE_LIMIT = 5


def counts_as_net_new_email(row: Dict[str, Any]) -> bool:
    email = (row.get("email") or "").strip().lower()
    if not email:
        return False
    status = (row.get("status") or "").strip().lower()
    if status and status not in {"email_ready", "mgmt_email"}:
        return False
    if bool(row.get("sent_to_smartlead")) and not row.get("reviewed_at"):
        return False
    return True


def classify_reviewable_email(email: str) -> Tuple[str, str]:
    normalized = (email or "").strip().lower()
    if "@" not in normalized:
        return "junk", "Email is blank or malformed."
    host = normalized.split("@", 1)[1]
    source_hint = f"https://{host}" if host else "bio"
    return classify_email(normalized, source_hint)


def scrub_invalid_reviewable_emails(supabase: SupabaseClient, *, limit: int = 500) -> Dict[str, Any]:
    if not supabase.enabled():
        return {"cleaned": 0, "by_kind": {}, "examples": []}

    try:
        rows = supabase.fetch_rows(
            "leads",
            select="id,instagram_handle,email,status,review_status,batch_date,sent_to_smartlead",
            filters=[
                "email=not.is.null",
                "review_status=eq.unreviewed",
                "reviewed_at=is.null",
                "status=in.(email_ready,mgmt_email)",
            ],
            limit=limit,
        )
    except Exception as exc:
        print(f"Invalid email scrub skipped: {exc}")
        return {"cleaned": 0, "by_kind": {}, "examples": [], "error": str(exc)}

    invalid_rows: List[Dict[str, Any]] = []
    by_kind: Counter[str] = Counter()
    examples: List[Dict[str, str]] = []
    for row in rows:
        if bool(row.get("sent_to_smartlead")):
            continue
        email = (row.get("email") or "").strip().lower()
        kind, reason = classify_reviewable_email(email)
        if is_reviewable_email_type(kind):
            continue
        invalid_rows.append(row)
        by_kind[kind] += 1
        if len(examples) < 8:
            examples.append(
                {
                    "handle": (row.get("instagram_handle") or "").strip(),
                    "email": email,
                    "kind": kind,
                    "day": (row.get("batch_date") or "").strip(),
                    "reason": reason,
                }
            )

    cleaned = 0
    if invalid_rows:
        try:
            for index in range(0, len(invalid_rows), 25):
                chunk = invalid_rows[index:index + 25]
                supabase.bulk_update_leads(
                    [str(row["id"]) for row in chunk],
                    {
                        "email": None,
                        "email_source": None,
                        "status": "no_email",
                        "review_status": "unreviewed",
                        "reviewed_at": None,
                        "reviewed_by": None,
                        "review_notes": None,
                    },
                )
                cleaned += len(chunk)
        except Exception as exc:
            print(f"Invalid email scrub failed: {exc}")
            return {"cleaned": 0, "by_kind": {}, "examples": [], "error": str(exc)}

    if cleaned:
        log_worker_event(
            supabase,
            "invalid_reviewable_emails_scrubbed",
            "warning",
            {
                "day": today_business_date(),
                "cleaned": cleaned,
                "by_kind": dict(by_kind),
                "examples": examples,
            },
        )
    return {
        "cleaned": cleaned,
        "by_kind": dict(by_kind),
        "examples": examples,
    }


def build_duplicate_guard(smartlead_api_key: str) -> Dict[str, Any]:
    emails: set[str] = set()
    if not smartlead_api_key:
        return {"smartlead_emails": emails, "supabase_email_cache": {}, "session_emails": {}}
    try:
        smartlead_index = build_account_lead_index(smartlead_api_key)
        emails = {str(email).strip().lower() for email in (smartlead_index.get("emails") or {}).keys() if str(email).strip()}
        print(f"Loaded {len(emails)} Smartlead emails for duplicate guarding.")
    except Exception as exc:
        print(f"Smartlead duplicate guard unavailable: {exc}")
    return {
        "smartlead_emails": emails,
        "supabase_email_cache": {},
        "session_emails": {},
    }


def normalize_counter_key(value: str | None, *, default: str = "unknown") -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized or default


def remember_session_email(
    duplicate_guard: Dict[str, Any] | None,
    handle: str,
    email: str,
) -> None:
    if not duplicate_guard:
        return
    session_emails = duplicate_guard.get("session_emails")
    if not isinstance(session_emails, dict):
        return
    normalized_email = (email or "").strip().lower()
    normalized_handle = (handle or "").strip().lower()
    if not normalized_email or not normalized_handle:
        return
    session_emails[normalized_email] = normalized_handle


def is_live_duplicate_email(
    supabase: SupabaseClient,
    duplicate_guard: Dict[str, Any] | None,
    handle: str,
    email: str,
) -> Tuple[bool, str]:
    normalized_handle = (handle or "").strip().lower()
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        return False, ""

    smartlead_emails = duplicate_guard.get("smartlead_emails") if duplicate_guard else set()
    if normalized_email in smartlead_emails:
        return True, "already exists in Smartlead"

    session_emails = duplicate_guard.get("session_emails") if duplicate_guard else None
    if isinstance(session_emails, dict):
        existing_handle = (session_emails.get(normalized_email) or "").strip().lower()
        if existing_handle and existing_handle != normalized_handle:
            return True, f"already belongs to @{existing_handle} in this run"

    if not supabase.enabled():
        return False, ""

    cache = duplicate_guard.get("supabase_email_cache") if duplicate_guard else None
    rows = None
    if isinstance(cache, dict):
        rows = cache.get(normalized_email)
    if rows is None:
        try:
            rows = supabase.fetch_existing_leads_by_email(normalized_email)
        except Exception:
            rows = []
        if isinstance(cache, dict):
            cache[normalized_email] = rows
    for row in rows or []:
        row_handle = (row.get("instagram_handle") or "").strip().lower()
        if row_handle and row_handle != normalized_handle:
            return True, f"already belongs to @{row_handle}"
    return False, ""


def filter_duplicate_email_candidates(
    supabase: SupabaseClient,
    duplicate_guard: Dict[str, Any] | None,
    counts: Counter,
    handle: str,
    candidates: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    usable: List[Dict[str, str]] = []
    for candidate in candidates:
        duplicate, reason = is_live_duplicate_email(supabase, duplicate_guard, handle, candidate.get("email") or "")
        if duplicate:
            if reason == "already exists in Smartlead":
                counts["duplicate_existing_smartlead"] += 1
            else:
                counts["duplicate_existing_live_email"] += 1
            print(f"  Skipped duplicate email {candidate.get('email')} for @{handle}: {reason}")
            continue
        usable.append(candidate)
    return usable


def live_row_blocks_processing(row: Dict[str, Any] | None) -> bool:
    if not row:
        return False
    if (row.get("email") or "").strip():
        return True
    if row.get("sent_to_smartlead") is True:
        return True
    if row.get("smartlead_campaign_id"):
        return True
    if row.get("smartlead_sent_at"):
        return True
    return False


def filter_existing_live_handles_before_enrich(
    supabase: SupabaseClient,
    handles: List[str],
) -> Tuple[List[str], int]:
    if not supabase.enabled() or not handles:
        return handles, 0

    live_rows: Dict[str, Dict[str, Any]] = {}
    chunk_size = 150
    normalized_handles = sorted({(handle or "").strip().lower() for handle in handles if (handle or "").strip()})
    for start in range(0, len(normalized_handles), chunk_size):
        chunk = normalized_handles[start : start + chunk_size]
        live_rows.update(supabase.fetch_existing_handles(chunk))

    filtered: List[str] = []
    skipped = 0
    for handle in handles:
        normalized_handle = (handle or "").strip().lower()
        if live_row_blocks_processing(live_rows.get(normalized_handle)):
            skipped += 1
            continue
        filtered.append(handle)
    return filtered, skipped


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return parsed


def validate_target_emails(target_emails: int, *, command_name: str) -> int:
    if target_emails <= 0:
        raise RuntimeError(
            f"{command_name} requires --target-emails > 0. Refusing to run a fake-success batch with target {target_emails}."
        )
    return target_emails


def daily_hard_stop_reached(day: str, hard_stop_hour_local: int, *, now: datetime | None = None) -> bool:
    timezone = business_tz()
    stop_at = datetime.strptime(day, "%Y-%m-%d").replace(
        hour=hard_stop_hour_local,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=timezone,
    )
    current = now or business_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone)
    else:
        current = current.astimezone(timezone)
    return current >= stop_at


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lean creator email finder v1")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the full discovery-to-export pipeline")
    run_parser.add_argument("--seeds", required=True, help="Comma-separated Instagram handles")
    run_parser.add_argument("--target-emails", type=positive_int, default=25)

    daily_parser = subparsers.add_parser("daily-run", help="Keep running until the daily target is hit or the day ends")
    daily_parser.add_argument("--target-emails", type=positive_int, default=DEFAULT_DAILY_EMAIL_TARGET)
    daily_parser.add_argument("--seed-batch-size", type=positive_int, default=DEFAULT_DAILY_BATCH_SIZE)
    daily_parser.add_argument("--max-cycles", type=positive_int, default=25)
    daily_parser.add_argument("--hard-stop-hour-local", type=int, default=22)

    check_doc_parser = subparsers.add_parser("check-doc", help="Poll pending DataOverCoffee jobs and harvest finished emails")
    check_doc_parser.add_argument("--limit", type=int, default=100, help="Maximum pending DOC jobs to check")

    reconcile_parser = subparsers.add_parser("reconcile-smartlead", help="Check Smartlead and mark sent leads back into Supabase")
    reconcile_parser.add_argument("--limit", type=int, default=DEFAULT_SMARTLEAD_RECONCILE_LIMIT, help="Maximum leads to check")
    reconcile_parser.add_argument("--historical", action="store_true", help="Backfill old emailed leads that were already uploaded to Smartlead")

    subparsers.add_parser("refresh-results", help="Reclassify saved email candidates and rebuild the clean export")
    subparsers.add_parser("import-dashboard-db", help="Import delivered emails from the dashboard Supabase into the main outreach database")
    subparsers.add_parser("repair-state", help="Close stale unfinished local runs after interruptions")
    status_parser = subparsers.add_parser("status", help="Print machine-readable operational status for today")
    status_parser.add_argument("--day", default=today_business_date(), help="Business day to inspect (YYYY-MM-DD)")
    subparsers.add_parser("doctor", help="Check backend readiness for cloud deployment")
    reels_parser = subparsers.add_parser("transcribe-reels", help="Transcribe recent Instagram reels for a handle")
    reels_parser.add_argument("--handle", required=True, help="Instagram handle")
    reels_parser.add_argument("--limit", type=int, default=8, help="Number of recent reel videos to transcribe")

    return parser.parse_args()


def discover_handles(apify: ApifyHTTPClient, seed: str) -> List[str]:
    run = apify.call_actor(SIMILAR_ACTOR, {"username": [seed]}, wait_secs=120)
    items = apify.get_dataset_items(run["defaultDatasetId"])
    handles = []
    for item in items:
        handle = (item.get("username") or "").strip().lower().replace("@", "")
        if handle and handle not in handles:
            handles.append(handle)
    return handles


def enrich_handles(apify: ApifyHTTPClient, handles: List[str]) -> List[Dict[str, Any]]:
    if not handles:
        return []
    run = apify.call_actor(ENRICH_ACTOR, {"usernames": handles}, wait_secs=300)
    items = apify.get_dataset_items(run["defaultDatasetId"])
    profiles = []
    for item in items:
        username = (item.get("username") or item.get("profileUsername") or "").strip().lower().replace("@", "")
        if not username:
            continue
        recent_captions = []
        for post in (item.get("latestPosts") or [])[:5]:
            caption = (post.get("caption") or "").replace("\n", " ").strip()
            if caption:
                recent_captions.append(caption[:220])
        youtube_url = first_youtube_url(
            " ".join(
                [
                    item.get("biography") or item.get("bio") or "",
                    item.get("externalUrl") or item.get("website") or "",
                    " ".join(recent_captions),
                ]
            )
        )
        profile = {
            "username": username,
            "full_name": item.get("full_name") or item.get("fullName") or item.get("name") or "",
            "biography": item.get("biography") or item.get("bio") or item.get("profileBio") or "",
            "followers": item.get("followersCount") or item.get("followers") or 0,
            "external_url": item.get("externalUrl") or item.get("website") or item.get("profileWebsite") or "",
            "business_category": item.get("businessCategoryName") or item.get("category") or "",
            "recent_captions": recent_captions,
            "youtube_url": youtube_url,
        }
        profiles.append(profile)
    return profiles


def unfinished_cached_profiles(db: FinderDB, seed: str) -> List[Dict[str, Any]]:
    rows = list(db.conn.execute(
        """
        select handle, profile_json
        from creators
        where source_seed = ?
          and profile_json is not null
          and best_email is null
          and (qualified is null or qualified = 1 or reject_reason = 'under_100k')
        order by coalesce(followers, 0) desc
        """,
        (seed,),
    ))
    profiles = []
    for row in rows:
        if not row["profile_json"]:
            continue
        profiles.append(json.loads(row["profile_json"]))
    return profiles


def discovered_without_profile(db: FinderDB, seed: str) -> List[str]:
    rows = db.conn.execute(
        """
        select handle
        from creators
        where source_seed = ?
          and profile_json is null
        order by handle asc
        """,
        (seed,),
    ).fetchall()
    return [row["handle"] for row in rows]


def first_youtube_url(text: str) -> str:
    match = re.search(r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s\"'>]+", text or "", re.I)
    return match.group(0) if match else ""


def parse_doc_candidates(items: List[Dict[str, Any]], youtube_url: str) -> List[Dict[str, str]]:
    candidates = []
    for item in items:
        email = (item.get("Email") or item.get("email") or "").strip()
        if not email:
            continue
        lowered = email.lower()
        email_type, notes = classify_email(lowered, youtube_url)
        candidates.append({
            "email": lowered,
            "email_type": email_type,
            "source_method": "youtube_fallback",
            "source_url": youtube_url,
            "source_path": "youtube",
            "notes": f"{notes} YouTube fallback result: {item.get('Status') or item.get('status') or 'EMAIL_AVAILABLE'}",
        })
    return candidates


def doc_status_text(payload: Dict[str, Any]) -> str:
    for key in ("status", "run_status", "state", "phase"):
        value = payload.get(key)
        if value:
            return str(value)
    return "unknown"


def doc_pending_count(payload: Dict[str, Any]) -> int | None:
    for key in ("pending", "pending_count", "remaining", "remaining_channels", "channels_pending"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def doc_is_done(payload: Dict[str, Any]) -> bool:
    for key in ("done", "complete", "completed", "finished", "is_done"):
        if payload.get(key) is True:
            return True
    status = doc_status_text(payload).lower()
    pending = doc_pending_count(payload)
    done_words = {"done", "complete", "completed", "finished", "succeeded", "success", "ready"}
    if status in done_words and (pending is None or pending == 0):
        return True
    return False


def doc_is_forbidden(payload: Dict[str, Any]) -> bool:
    status = doc_status_text(payload).lower()
    return payload.get("_http_status") == 403 or status == "forbidden"


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def should_resurrect_doc(job: Dict[str, Any], apify_status: str, doc_payload: Dict[str, Any]) -> bool:
    if apify_status.upper() not in {"SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"}:
        return False
    if doc_is_forbidden(doc_payload):
        return False
    if doc_is_done(doc_payload):
        return False
    if int(job.get("resurrect_count") or 0) >= 2:
        return False
    last_checked = parse_iso_utc(job.get("last_checked_at"))
    if last_checked and datetime.now(timezone.utc) - last_checked < timedelta(minutes=15):
        return False
    return True


def should_close_doc_job_as_terminal_forbidden(
    job: Dict[str, Any],
    doc_payload: Dict[str, Any],
    *,
    results_collected: int,
) -> bool:
    if not doc_is_forbidden(doc_payload):
        return False
    if results_collected > 0:
        return False
    if int(job.get("resurrect_count") or 0) >= 2:
        return True
    submitted_at = parse_iso_utc(job.get("submitted_at"))
    if submitted_at and datetime.now(timezone.utc) - submitted_at >= timedelta(hours=DOC_FORBIDDEN_TERMINAL_AFTER_HOURS):
        return True
    return False


def has_terminal_doc_forbidden(job: Dict[str, Any]) -> bool:
    if str(job.get("doc_status") or "").strip().lower() != "forbidden":
        return False
    if int(job.get("results_collected") or 0) > 0:
        return False
    submitted_at = parse_iso_utc(job.get("submitted_at"))
    if not submitted_at:
        return False
    return datetime.now(timezone.utc) - submitted_at >= timedelta(hours=DOC_FORBIDDEN_TERMINAL_AFTER_HOURS)


def submit_doc_job(
    apify: ApifyHTTPClient,
    db: FinderDB,
    handle: str,
    youtube_url: str,
) -> Tuple[Dict[str, Any] | None, List[Dict[str, str]], bool]:
    youtube_url = (youtube_url or "").strip()
    if "youtube" not in youtube_url.lower():
        return None, [], False
    existing_job = db.get_open_doc_job(handle, youtube_url)
    if existing_job:
        if has_terminal_doc_forbidden(dict(existing_job)):
            db.mark_doc_job_completed(
                existing_job["apify_run_id"],
                "Closed stale forbidden DOC job before submitting a fresh attempt.",
            )
        else:
            dataset_id = existing_job["dataset_id"]
            items = apify.get_dataset_items(dataset_id) if dataset_id else []
            candidates = parse_doc_candidates(items, youtube_url)
            db.update_doc_job_status(
                existing_job["apify_run_id"],
                results_collected=len(candidates),
                notes="Reused existing pending DOC job.",
            )
            return dict(existing_job), candidates, False
    run = apify.call_actor(DOC_ACTOR, {"channels": [youtube_url]}, wait_secs=300)
    run_id = run["id"]
    dataset_id = run.get("defaultDatasetId") or ""
    apify_status = run.get("status") or "UNKNOWN"
    db.upsert_doc_job(handle, youtube_url, run_id, dataset_id, apify_status, apify_status, "Initial submission.")
    items = apify.get_dataset_items(dataset_id) if dataset_id else []
    candidates = parse_doc_candidates(items, youtube_url)
    db.update_doc_job_status(run_id, results_collected=len(candidates), notes="Initial submission harvested.")
    return {
        "creator_handle": handle,
        "youtube_channel": youtube_url,
        "apify_run_id": run_id,
        "dataset_id": dataset_id,
        "apify_status": apify_status,
    }, candidates, True


def sync_youtube_state_to_supabase(
    supabase: SupabaseClient,
    profile: Dict[str, Any],
    source_seed: str,
    *,
    batch_date: str | None = None,
) -> None:
    if not supabase.enabled() or not profile.get("youtube_url"):
        return
    payload = {
        "instagram_handle": profile["username"],
        "full_name": profile.get("full_name"),
        "instagram_url": f"https://instagram.com/{profile['username']}",
        "follower_count": int(profile.get("followers") or 0),
        "status": "youtube_only",
        "batch_date": batch_date or today_business_date(),
        "source": "finder_v1",
        "source_detail": profile.get("source_detail") or source_seed,
        "bio": profile.get("biography"),
        "business_category": profile.get("business_category"),
        "external_url": profile.get("external_url"),
        "youtube_channel": profile.get("youtube_url"),
    }
    supabase.upsert_lead(payload)


def filter_existing_live_email_profiles(
    supabase: SupabaseClient,
    profiles: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    if not supabase.enabled() or not profiles:
        return profiles, 0

    live_rows: Dict[str, Dict[str, Any]] = {}
    handles = sorted({(profile.get("username") or "").strip().lower() for profile in profiles if profile.get("username")})
    chunk_size = 150
    for start in range(0, len(handles), chunk_size):
        chunk = handles[start : start + chunk_size]
        live_rows.update(supabase.fetch_existing_handles(chunk))

    filtered: List[Dict[str, Any]] = []
    skipped = 0
    for profile in profiles:
        handle = (profile.get("username") or "").strip().lower()
        live_row = live_rows.get(handle)
        if live_row_blocks_processing(live_row):
            skipped += 1
            continue
        filtered.append(profile)
    return filtered, skipped


def clean_sheet_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def write_csvs(db: FinderDB, run_id: int) -> Dict[str, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    final_path = OUTPUT_DIR / f"run_{run_id}_results.csv"
    audit_path = OUTPUT_DIR / f"run_{run_id}_audit.csv"

    export_rows = list(db.iter_export_rows())
    with final_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "instagram_handle", "seed_handle", "followers", "email", "email_type",
            "email_source", "website", "notes",
        ])
        for row in export_rows:
            if row["best_email"] and is_reviewable_email_type(row["best_email_type"]):
                profile = json.loads(row["profile_json"]) if row["profile_json"] else {}
                writer.writerow([
                    row["handle"],
                    row["source_seed"],
                    row["followers"] or "",
                    row["best_email"],
                    row["best_email_type"],
                    row["best_email_source"] or "",
                    profile.get("external_url", ""),
                    row["best_email_notes"] or "",
                ])

    with audit_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "instagram_handle", "seed_handle", "followers", "stage", "qualified",
            "reject_reason", "best_email", "email_type", "email_source",
        ])
        for row in export_rows:
            writer.writerow([
                row["handle"],
                row["source_seed"],
                row["followers"] or "",
                row["stage"],
                row["qualified"],
                row["reject_reason"] or "",
                row["best_email"] or "",
                row["best_email_type"] or "",
                row["best_email_source"] or "",
            ])

    return {"final": str(final_path), "audit": str(audit_path)}


def write_new_results_csv(db: FinderDB, run_id: int, started_at: str) -> str:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"run_{run_id}_new_results.csv"
    rows = list(db.iter_new_export_rows(started_at))
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "instagram_handle", "seed_handle", "followers", "email", "email_type",
            "email_source", "email_found_at", "website", "notes",
        ])
        for row in rows:
            profile = json.loads(row["profile_json"]) if row["profile_json"] else {}
            writer.writerow([
                row["handle"],
                row["source_seed"],
                row["followers"] or "",
                row["best_email"],
                row["best_email_type"],
                row["best_email_source"] or "",
                row["best_email_found_at"] or "",
                profile.get("external_url", ""),
                row["best_email_notes"] or "",
            ])
    return str(path)


def write_daily_review_csv(db: FinderDB, run_id: int, started_at: str) -> str:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    review_path = OUTPUT_DIR / f"run_{run_id}_review.csv"
    rows = db.conn.execute(
        """
        select handle, source_seed, followers, best_email, best_email_type, best_email_source,
               profile_json, updated_at
        from creators
        where coalesce(best_email_found_at, contact_searched_at, discovered_at) >= ?
          and best_email is not null
          and best_email_type in ('personal', 'management', 'generic_business', 'brand')
        order by coalesce(followers, 0) desc, handle asc
        """,
        (started_at,),
    ).fetchall()
    with review_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "Qualified? Y/N",
            "First name",
            "Instagram clickable link",
            "Bio",
            "Email",
            "Email type",
            "Seed handle",
        ])
        for row in rows:
            profile = json.loads(row["profile_json"]) if row["profile_json"] else {}
            full_name = clean_sheet_text(profile.get("full_name") or "")
            first_name = full_name.split(" ")[0] if full_name else ""
            writer.writerow([
                "",
                first_name,
                f"https://instagram.com/{row['handle']}",
                clean_sheet_text(profile.get("biography") or ""),
                row["best_email"] or "",
                row["best_email_type"] or "",
                row["source_seed"] or "",
            ])
    return str(review_path)


def checkpoint_outputs(db: FinderDB, run_id: int, counts: Counter) -> Dict[str, str]:
    refresh_saved_results(db)
    paths = write_csvs(db, run_id)
    db.checkpoint_run(run_id, dict(counts), paths["final"], paths["audit"])
    return paths


def checkpoint_outputs_with_review(db: FinderDB, run_id: int, counts: Counter, started_at: str) -> Dict[str, str]:
    paths = checkpoint_outputs(db, run_id, counts)
    paths["new_results"] = write_new_results_csv(db, run_id, started_at)
    review_path = write_daily_review_csv(db, run_id, started_at)
    return {**paths, "review": review_path}


def supabase_row_to_profile(row: Dict[str, Any]) -> Dict[str, Any]:
    handle = (row.get("instagram_handle") or "").strip().lower()
    return {
        "username": handle,
        "full_name": row.get("full_name") or "",
        "biography": row.get("bio") or "",
        "followers": row.get("follower_count") or 0,
        "external_url": row.get("external_url") or "",
        "business_category": row.get("business_category") or "",
        "recent_captions": [],
        "youtube_url": row.get("youtube_channel") or "",
        "source_detail": row.get("source_detail") or "supabase_inventory",
    }


def sync_best_to_supabase(
    supabase: SupabaseClient,
    counts: Counter,
    handle: str,
    followers: int,
    profile: Dict[str, Any],
    best: Dict[str, str],
    source_seed: str,
    *,
    batch_date: str | None = None,
) -> None:
    payload = {
        "instagram_handle": handle,
        "full_name": profile.get("full_name"),
        "instagram_url": f"https://instagram.com/{handle}",
        "follower_count": followers,
        "email": best["email"],
        "email_source": best["source_method"],
        "status": "mgmt_email" if best["email_type"] == "management" else "email_ready",
        "batch_date": batch_date or today_business_date(),
        "source": "finder_v1",
        "source_detail": profile.get("source_detail") or source_seed,
        "bio": profile.get("biography"),
        "business_category": profile.get("business_category"),
        "external_url": profile.get("external_url"),
        "youtube_channel": profile.get("youtube_url") or None,
        "qualification_reasoning": "qualified",
        "first_name": (profile.get("full_name") or "").split(" ")[0] if profile.get("full_name") else None,
    }
    try:
        supabase.upsert_lead(payload)
        counts["synced_to_supabase"] += 1
    except Exception as exc:
        counts["supabase_sync_errors"] += 1
        print(f"  Supabase sync failed for @{handle}: {exc}")


def clear_creator_email_in_supabase(
    supabase: SupabaseClient,
    counts: Counter,
    handle: str,
    profile: Dict[str, Any],
    source_seed: str,
    *,
    batch_date: str | None = None,
) -> None:
    payload = {
        "instagram_handle": handle,
        "full_name": profile.get("full_name"),
        "instagram_url": f"https://instagram.com/{handle}",
        "follower_count": int(profile.get("followers") or 0),
        "email": None,
        "email_source": None,
        "status": "youtube_only" if profile.get("youtube_url") else "no_email",
        "batch_date": batch_date or today_business_date(),
        "source": "finder_v1",
        "source_detail": profile.get("source_detail") or source_seed,
        "bio": profile.get("biography"),
        "business_category": profile.get("business_category"),
        "external_url": profile.get("external_url"),
        "youtube_channel": profile.get("youtube_url") or None,
    }
    try:
        supabase.upsert_lead(payload)
        counts["supabase_cleared"] += 1
    except Exception as exc:
        counts["supabase_sync_errors"] += 1
        print(f"  Supabase clear failed for @{handle}: {exc}")


def mark_smartlead_sent(
    supabase: SupabaseClient,
    row: Dict[str, Any],
    smartlead_payload: Dict[str, Any],
    *,
    actor_role: str,
    actor_identifier: str,
) -> bool:
    lead_id = str(row.get("id") or "").strip()
    if not lead_id:
        return False
    patch: Dict[str, Any] = {
        "sent_to_smartlead": True,
        "smartlead_sent_at": row.get("smartlead_sent_at") or utc_now_iso(),
    }
    campaign_id = extract_campaign_id(smartlead_payload)
    if campaign_id:
        patch["smartlead_campaign_id"] = campaign_id
    current_status = (row.get("review_status") or "").strip()
    if current_status in {"approved", "exported_pending_confirmation"}:
        patch["review_status"] = "approved"
    supabase.update_lead_by_id(lead_id, patch)
    try:
        supabase.insert_lead_review_event(
            {
                "lead_id": lead_id,
                "actor_role": actor_role,
                "actor_identifier": actor_identifier,
                "action": "smartlead_confirmed",
                "payload": {
                    "email": row.get("email"),
                    "instagram_handle": row.get("instagram_handle"),
                    "campaign_id": campaign_id,
                    "previous_review_status": current_status,
                },
            }
        )
    except Exception:
        pass
    return True


def reconcile_smartlead(
    supabase: SupabaseClient,
    smartlead_api_key: str,
    *,
    limit: int,
    historical: bool,
) -> Dict[str, Any]:
    if not supabase.enabled():
        raise RuntimeError("Supabase credentials are not configured.")
    if not smartlead_api_key:
        raise RuntimeError("SMARTLEAD_API_KEY is not configured.")

    smartlead_index = build_account_lead_index(smartlead_api_key)
    summary = Counter()
    summary["smartlead_campaigns"] = int(smartlead_index.get("campaign_count") or 0)
    summary["smartlead_leads_indexed"] = int(smartlead_index.get("lead_count") or 0)

    matched_rows: List[Dict[str, Any]] = []
    unmatched_rows: List[Dict[str, Any]] = []

    remaining = max(limit, 0)
    offset = 0
    page_size = min(500, max(100, remaining or 500))
    emails_index = smartlead_index.get("emails") or {}
    handles_index = smartlead_index.get("handles") or {}

    while True:
        batch_limit = page_size if remaining == 0 else min(page_size, remaining)
        if batch_limit <= 0:
            break
        rows = supabase.fetch_smartlead_sync_candidates(limit=batch_limit, offset=offset)
        if not rows:
            break
        summary["candidates"] += len(rows)
        for row in rows:
            email = (row.get("email") or "").strip().lower()
            handle = normalize_instagram_handle(row.get("instagram_handle"))
            email_match = emails_index.get(email) if email else None
            handle_match = handles_index.get(handle) if handle else None
            match = email_match or handle_match
            summary["checked"] += 1
            if match:
                payload = {
                    "campaign_id": next(iter(match.get("campaign_ids") or []), ""),
                    "memberships": match.get("memberships") or [],
                }
                if mark_smartlead_sent(
                    supabase,
                    row,
                    payload,
                    actor_role="worker",
                    actor_identifier="finder_v1_worker",
                ):
                    summary["confirmed_sent"] += 1
                    if email_match:
                        summary["matched_by_email"] += 1
                    elif handle_match:
                        summary["matched_by_handle"] += 1
                    matched_rows.append(
                        {
                            "id": row.get("id"),
                            "email": email,
                            "instagram_handle": row.get("instagram_handle"),
                            "campaign_id": extract_campaign_id(payload),
                            "match_type": "email" if email_match else "handle",
                        }
                    )
                continue
            summary["still_pending"] += 1
            unmatched_rows.append(
                {
                    "id": row.get("id"),
                    "email": email,
                    "instagram_handle": row.get("instagram_handle"),
                    "review_status": row.get("review_status"),
                }
            )
        offset += len(rows)
        if remaining:
            remaining -= len(rows)
            if remaining <= 0:
                break
        if len(rows) < batch_limit:
            break
    return {
        "historical": historical,
        "rate_limited": False,
        "summary": dict(summary),
        "matched": matched_rows,
        "unmatched": unmatched_rows,
    }


def compact_smartlead_reconcile_event_data(
    result: Dict[str, Any],
    *,
    limit: int,
    historical: bool,
) -> Dict[str, Any]:
    matched_rows = result.get("matched") or []
    unmatched_rows = result.get("unmatched") or []
    payload: Dict[str, Any] = {
        "day": today_business_date(),
        "historical": historical,
        "limit": limit,
        "rate_limited": bool(result.get("rate_limited")),
        "summary": dict(result.get("summary") or {}),
        "matched_count": len(matched_rows),
        "unmatched_count": len(unmatched_rows),
    }
    if matched_rows:
        payload["matched_sample"] = matched_rows[:WORKER_EVENT_SAMPLE_LIMIT]
    if unmatched_rows:
        payload["unmatched_sample"] = unmatched_rows[:WORKER_EVENT_SAMPLE_LIMIT]
    if result.get("invalid_email_scrub"):
        payload["invalid_email_scrub"] = result["invalid_email_scrub"]
    return payload


def refresh_saved_results(db: FinderDB, supabase: SupabaseClient | None = None) -> Counter:
    counts = Counter()
    for row in db.iter_email_candidates():
        email_type, notes = classify_email(row["email"], row["source_url"] or "")
        keep = is_reviewable_email_type(email_type)
        if email_type != row["email_type"] or int(bool(row["keep"])) != int(keep) or notes != (row["notes"] or ""):
            db.update_email_candidate(row["id"], email_type, keep, notes)
            counts["candidates_reclassified"] += 1

    for handle in db.list_creator_handles():
        creator = db.get_creator(handle)
        if not creator:
            continue
        previous_best = creator["best_email"]
        previous_type = creator["best_email_type"]
        candidates = [
            {
                "email": row["email"],
                "email_type": row["email_type"],
                "source_method": row["source_method"],
                "source_url": row["source_url"] or "",
                "source_path": row["source_path"] or "",
                "notes": row["notes"] or "",
            }
            for row in db.list_email_candidates_for_creator(handle)
            if row["keep"] and is_reviewable_email_type(row["email_type"])
        ]
        best = best_candidate(candidates)
        if best:
            db.set_best_contact_snapshot(handle, best["email"], best["email_type"], best["source_method"], best["notes"])
        else:
            db.set_best_contact_snapshot(handle, None, None, None, "No kept email found after refresh.")

        current = db.get_creator(handle)
        if not current:
            continue
        if current["best_email"] != previous_best or current["best_email_type"] != previous_type:
            counts["creators_refreshed"] += 1
            if supabase and supabase.enabled():
                profile = db.get_profile(handle)
                if current["best_email"] and profile:
                    sync_best_to_supabase(
                        supabase,
                        counts,
                        handle,
                        int(profile.get("followers") or 0),
                        profile,
                        {
                            "email": current["best_email"],
                            "email_type": current["best_email_type"],
                            "source_method": current["best_email_source"] or "",
                            "notes": current["best_email_notes"] or "",
                        },
                        current["source_seed"] or "refresh_results",
                    )
                elif profile:
                    clear_creator_email_in_supabase(
                        supabase,
                        counts,
                        handle,
                        profile,
                        current["source_seed"] or "refresh_results",
                    )
    return counts


def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def import_dashboard_database() -> Dict[str, Any]:
    main_cfg = api_config()
    target = SupabaseClient(main_cfg["supabase_url"], main_cfg["supabase_service_role_key"])
    if not target.enabled():
        raise RuntimeError("Main Supabase credentials are not configured.")

    dashboard_env = load_env_file(Path("../All/AI Assets/Claude Code Experiment/dashboard/.env.local"))
    source = SupabaseClient(
        dashboard_env.get("SUPABASE_URL") or dashboard_env.get("NEXT_PUBLIC_SUPABASE_URL", ""),
        dashboard_env.get("SUPABASE_SERVICE_ROLE_KEY", ""),
    )
    if not source.enabled():
        raise RuntimeError("Dashboard Supabase credentials are not available.")

    delivered = source.fetch_rows("delivered_emails", select="id,email,username,job_id,created_at")
    jobs = source.fetch_rows("lead_jobs", select="id,results,status,email_count,lead_count,updated_at", filters=["status=eq.complete"])
    job_map = {row["id"]: row for row in jobs}

    usernames = sorted({(row.get("username") or "").strip().lower() for row in delivered if row.get("username")})
    existing_rows = target.fetch_existing_handles(usernames)

    counts = Counter()
    for row in delivered:
        handle = (row.get("username") or "").strip().lower()
        email = (row.get("email") or "").strip().lower()
        if not handle or not email:
            counts["skipped_incomplete"] += 1
            continue

        existing = existing_rows.get(handle)
        if existing and existing.get("email"):
            counts["skipped_existing_email"] += 1
            continue

        job = job_map.get(row.get("job_id"))
        profile = None
        if job:
            for item in job.get("results") or []:
                if (item.get("username") or "").strip().lower() == handle:
                    profile = item
                    break

        source_url = ""
        if profile:
            source_url = profile.get("website") or profile.get("profileUrl") or ""
        email_type, notes = classify_email(email, source_url)
        if not is_reviewable_email_type(email_type):
            counts["skipped_low_quality"] += 1
            continue

        payload = {
            "instagram_handle": handle,
            "full_name": profile.get("fullName") if profile else None,
            "instagram_url": profile.get("profileUrl") if profile else f"https://instagram.com/{handle}",
            "follower_count": profile.get("followers") if profile else None,
            "email": email,
            "email_source": "ccos_delivered_email",
            "status": "mgmt_email" if email_type == "management" else "email_ready",
            "batch_date": today_business_date(),
            "source": "ccos_dashboard",
            "source_detail": f"dashboard_job:{row.get('job_id')}",
            "bio": profile.get("biography") if profile else None,
            "business_category": profile.get("businessCategory") if profile else None,
            "external_url": profile.get("website") if profile else None,
            "youtube_channel": None,
            "qualification_reasoning": "imported_from_ccos_dashboard",
            "notes": notes,
            "email_verified": False,
        }
        target.upsert_lead(payload)
        counts["imported"] += 1

    return {
        "dashboard_delivered_rows": len(delivered),
        "dashboard_complete_jobs": len(jobs),
        **dict(counts),
    }


def transcribe_reels_for_handle(handle: str, limit: int) -> Dict[str, Any]:
    cfg = api_config()
    apify = ApifyHTTPClient(cfg["apify_token"])
    groq = GroqHTTPClient(cfg["groq_key"])
    if not apify.enabled():
        raise RuntimeError("APIFY_API_TOKEN is not configured")
    if not groq.enabled():
        raise RuntimeError("GROQ_API_KEY is not configured")

    run = apify.call_actor(ENRICH_ACTOR, {"usernames": [handle]}, wait_secs=300)
    items = apify.get_dataset_items(run["defaultDatasetId"])
    if not items:
        raise RuntimeError(f"No Instagram profile found for @{handle}")

    profile = items[0]
    reels = profile_reel_posts(profile, limit=limit)
    if not reels:
        raise RuntimeError(f"No reel videos found for @{handle}")

    transcript_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    for idx, reel in enumerate(reels, 1):
        try:
            media = download_media(reel["video_url"])
            prompt = (
                f"Instagram fitness reel from @{handle}. Preserve names, brand names, fitness terms, "
                "exercise names, supplements, slang, and coaching vocabulary."
            )
            response = groq.transcribe(
                media,
                f"{handle}_{idx}.mp4",
                media_url=reel["video_url"],
                prompt=prompt,
            )
        except Exception as exc:
            skipped_rows.append(
                {
                    "index": idx,
                    "short_code": reel["short_code"],
                    "instagram_url": reel["instagram_url"],
                    "caption": reel["caption"],
                    "video_url": reel["video_url"],
                    "timestamp": reel["timestamp"],
                    "error": str(exc),
                }
            )
            continue

        transcript_rows.append(
            {
                "index": idx,
                "short_code": reel["short_code"],
                "instagram_url": reel["instagram_url"],
                "caption": reel["caption"],
                "video_url": reel["video_url"],
                "timestamp": reel["timestamp"],
                "transcript": response.get("text", "").strip(),
                "language": response.get("language"),
                "duration": response.get("duration"),
                "segments": response.get("segments") or [],
                "words": response.get("words") or [],
            }
        )

    if not transcript_rows:
        sample_error = skipped_rows[0]["error"] if skipped_rows else "No reels could be transcribed."
        raise RuntimeError(f"No Tyson transcripts generated yet. First error: {sample_error}")

    payload = {
        "handle": handle,
        "profile_url": profile.get("url") or f"https://instagram.com/{handle}",
        "full_name": profile.get("fullName") or profile.get("full_name") or "",
        "followers": profile.get("followersCount") or 0,
        "reel_count_transcribed": len(transcript_rows),
        "reel_count_skipped": len(skipped_rows),
        "transcripts": transcript_rows,
        "skipped": skipped_rows,
    }
    paths = transcript_output_paths(OUTPUT_DIR.parent, handle)
    write_json(paths["json"], payload)
    write_simple_pdf(paths["pdf"], f"Instagram Reel Transcripts: @{handle}", transcript_rows)
    return {
        "handle": handle,
        "reel_count_transcribed": len(transcript_rows),
        "reel_count_skipped": len(skipped_rows),
        "json_path": str(paths["json"]),
        "pdf_path": str(paths["pdf"]),
    }


def harvest_doc_job(
    db: FinderDB,
    counts: Counter,
    apify: ApifyHTTPClient,
    supabase: SupabaseClient,
    duplicate_guard: Dict[str, Any] | None,
    job: Dict[str, Any],
    *,
    batch_date: str | None = None,
) -> bool:
    handle = job["creator_handle"]
    creator = db.get_creator(handle)

    try:
        run = apify.get_run(job["apify_run_id"])
    except Exception as exc:
        db.update_doc_job_status(job["apify_run_id"], error=f"Apify run check failed: {exc}")
        counts["doc_status_errors"] += 1
        counts["doc_apify_run_lookup_errors"] += 1
        return False
    counts[f"doc_apify_status_{normalize_counter_key(run.get('status'))}"] += 1

    try:
        doc_payload = apify.get_doc_run_status(job["apify_run_id"])
    except Exception as exc:
        doc_payload = {}
        db.update_doc_job_status(job["apify_run_id"], error=f"DOC status check failed: {exc}")
        counts["doc_status_errors"] += 1
        counts["doc_payload_lookup_errors"] += 1
    else:
        if doc_payload.get("_http_status") == 403:
            counts["doc_status_unavailable"] += 1
    doc_status_key = normalize_counter_key(doc_status_text(doc_payload))
    if doc_is_forbidden(doc_payload):
        counts["doc_status_forbidden"] += 1
    elif doc_payload:
        counts[f"doc_status_{doc_status_key}"] += 1
    else:
        counts["doc_status_empty_payload"] += 1

    dataset_id = run.get("defaultDatasetId") or job.get("dataset_id") or ""
    items = apify.get_dataset_items(dataset_id) if dataset_id else []
    candidates = parse_doc_candidates(items, job["youtube_channel"])
    db.update_doc_job_status(
        job["apify_run_id"],
        apify_status=run.get("status") or job.get("apify_status") or "UNKNOWN",
        doc_status=doc_status_text(doc_payload),
        dataset_id=dataset_id,
        results_collected=len(candidates),
        notes=json.dumps(doc_payload)[:400] if doc_payload else "No DOC status payload.",
    )

    new_candidates = []
    for candidate in candidates:
        if db.has_email_candidate(handle, candidate["email"], candidate["source_method"]):
            continue
        keep = is_reviewable_email_type(candidate["email_type"])
        db.add_email_candidate(
            handle,
            candidate["email"],
            candidate["email_type"],
            candidate["source_method"],
            candidate["source_url"],
            candidate["source_path"],
            keep,
            candidate["notes"],
        )
        new_candidates.append(candidate)

    usable_candidates = filter_duplicate_email_candidates(supabase, duplicate_guard, counts, handle, new_candidates)
    if new_candidates and not usable_candidates:
        counts["doc_candidates_duplicate_or_unusable"] += 1
    best = best_candidate(usable_candidates)
    if best:
        profile = db.get_profile(handle)
        followers = int((profile.get("followers") or 0) if profile else (creator["followers"] if creator else 0))
        db.record_contact_result(handle, best["email"], best["email_type"], best["source_method"], best["notes"])
        remember_session_email(duplicate_guard, handle, best["email"])
        counts["doc_emails_harvested"] += 1
        if supabase.enabled() and profile:
            sync_best_to_supabase(
                supabase,
                counts,
                handle,
                followers,
                profile,
                best,
                creator["source_seed"] if creator and creator["source_seed"] else "doc_harvest",
                batch_date=batch_date,
            )
        db.mark_doc_job_completed(job["apify_run_id"], "DOC returned a kept email.")
        print(f"  DOC harvested {best['email']} for @{handle}")
        return True

    apify_status = (run.get("status") or "").upper()
    if should_close_doc_job_as_terminal_forbidden(job, doc_payload, results_collected=len(candidates)):
        db.mark_doc_job_completed(
            job["apify_run_id"],
            "DOC status endpoint remained forbidden after repeated checks; closing stale job.",
        )
        counts["doc_terminal_forbidden"] += 1
        return False
    if should_resurrect_doc(job, apify_status, doc_payload):
        resurrected = apify.resurrect_run(job["apify_run_id"], wait_secs=30)
        new_run_id = resurrected.get("id") or job["apify_run_id"]
        db.increment_doc_job_resurrect_count(
            job["apify_run_id"],
            new_run_id,
            resurrected.get("defaultDatasetId") or dataset_id,
            resurrected.get("status") or apify_status,
            "Resurrected DOC run to collect late results.",
        )
        counts["doc_resurrected"] += 1
        return False

    if doc_is_done(doc_payload):
        db.mark_doc_job_completed(job["apify_run_id"], "DOC run completed without a kept email.")
        counts["doc_completed_no_email"] += 1
        if candidates:
            counts["doc_done_with_candidates_but_no_kept_email"] += 1
        else:
            counts["doc_done_without_candidates"] += 1
    else:
        counts["doc_pending"] += 1
    return False


def check_doc_jobs(
    db: FinderDB,
    counts: Counter,
    apify: ApifyHTTPClient,
    supabase: SupabaseClient,
    duplicate_guard: Dict[str, Any] | None,
    limit: int = DEFAULT_DOC_CHECK_LIMIT,
    time_budget_seconds: int = DEFAULT_DOC_CHECK_TIME_BUDGET_SECONDS,
    batch_date: str | None = None,
) -> int:
    harvested = 0
    counts["doc_pending_before_check"] = db.count_pending_doc_jobs()
    jobs = db.list_pending_doc_jobs()[:limit]
    if jobs:
        print(f"Checking {len(jobs)} pending DOC job(s). Backlog before check: {counts['doc_pending_before_check']}")
    else:
        print("Checking DOC jobs. Backlog before check: 0")
    deadline = time.monotonic() + max(time_budget_seconds, 1)
    for job in jobs:
        if time.monotonic() >= deadline:
            counts["doc_poll_budget_exhausted"] += 1
            print("  Stopping DOC polling for now to keep the daily run moving.")
            break
        try:
            if harvest_doc_job(db, counts, apify, supabase, duplicate_guard, dict(job), batch_date=batch_date):
                harvested += 1
        except Exception as exc:
            counts["doc_job_errors"] += 1
            db.update_doc_job_status(job["apify_run_id"], error=f"DOC harvest failed: {exc}")
            print(f"  DOC check failed for @{job['creator_handle']}: {exc}")
    counts["doc_pending_after_check"] = db.count_pending_doc_jobs()
    print(
        "DOC check complete: "
        f"harvested={harvested}, pending_after={counts['doc_pending_after_check']}, "
        f"errors={counts['doc_job_errors'] + counts['doc_status_errors'] + counts['doc_submit_errors']}"
    )
    return harvested


def safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def build_seed_history(db: FinderDB, day: str) -> List[Dict[str, Any]]:
    usage_map = {row["seed_handle"]: dict(row) for row in db.list_seed_usage_for_day(day)}
    perf_map = {row["seed_handle"]: dict(row) for row in db.list_seed_performance()}
    history: List[Dict[str, Any]] = []
    for seed in sorted(set(usage_map) | set(perf_map)):
        perf = perf_map.get(seed, {})
        usage = usage_map.get(seed, {})
        total_discovered = int(perf.get("total_discovered") or 0)
        total_enriched = int(perf.get("total_enriched") or 0)
        total_qualified = int(perf.get("total_qualified") or 0)
        total_kept = int(perf.get("total_kept") or 0)
        total_skipped_existing_live = int(perf.get("total_skipped_existing_live") or 0)
        total_duplicate_emails = int(perf.get("total_duplicate_emails") or 0)
        total_doc_harvested = int(perf.get("total_doc_harvested") or 0)
        kept_rate = safe_rate(total_kept, max(total_enriched, total_discovered, 1))
        qualified_rate = safe_rate(total_qualified, max(total_enriched, total_discovered, 1))
        doc_rate = safe_rate(total_doc_harvested, max(total_kept, 1))
        live_skip_rate = safe_rate(total_skipped_existing_live, max(total_discovered, 1))
        duplicate_rate = safe_rate(total_duplicate_emails, max(total_qualified, total_enriched, total_discovered, 1))
        history.append(
            {
                "seed_handle": seed,
                "used_today": bool(usage),
                "last_outcome_today": usage.get("last_outcome") or "",
                "total_runs": int(perf.get("total_runs") or 0),
                "total_discovered": total_discovered,
                "total_enriched": total_enriched,
                "total_qualified": total_qualified,
                "total_kept": total_kept,
                "total_skipped_existing_live": total_skipped_existing_live,
                "total_duplicate_emails": total_duplicate_emails,
                "total_doc_harvested": total_doc_harvested,
                "qualified_rate": round(qualified_rate, 4),
                "kept_rate": round(kept_rate, 4),
                "live_skip_rate": round(live_skip_rate, 4),
                "duplicate_rate": round(duplicate_rate, 4),
                "doc_rate": round(doc_rate, 4),
                "last_used_at": perf.get("last_used_at") or "",
            }
        )
    return history


def select_seed_batch(
    supabase: SupabaseClient,
    batch_size: int,
) -> List[str]:
    if not supabase.enabled():
        return []
    seeds = supabase.fetch_unexpanded_qualified_seed_handles(limit=max(batch_size * 20, batch_size, 100))
    return seeds[:batch_size]


def write_daily_seed_report_csv(db: FinderDB, day: str) -> str:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"daily_{day}_seed_report.csv"
    rows = build_seed_history(db, day)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "seed_handle",
            "used_today",
            "last_outcome_today",
            "total_runs",
            "total_discovered",
            "total_enriched",
            "total_qualified",
            "total_kept",
            "total_skipped_existing_live",
            "total_duplicate_emails",
            "total_doc_harvested",
            "qualified_rate",
            "kept_rate",
            "live_skip_rate",
            "duplicate_rate",
            "doc_rate",
            "last_used_at",
        ])
        for row in rows:
            writer.writerow([
                row["seed_handle"],
                "y" if row["used_today"] else "n",
                row["last_outcome_today"],
                row["total_runs"],
                row["total_discovered"],
                row["total_enriched"],
                row["total_qualified"],
                row["total_kept"],
                row["total_skipped_existing_live"],
                row["total_duplicate_emails"],
                row["total_doc_harvested"],
                row["qualified_rate"],
                row["kept_rate"],
                row["live_skip_rate"],
                row["duplicate_rate"],
                row["doc_rate"],
                row["last_used_at"],
            ])
    return str(path)


def write_daily_summary_json(
    db: FinderDB,
    run_id: int,
    day: str,
    started_at: str,
    target_emails: int,
    counts: Counter,
    latest_seed_batch: List[str],
    paths: Dict[str, str],
    supabase: SupabaseClient,
    schema: Dict[str, Any] | None = None,
) -> str:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"daily_{day}_summary.json"
    effective_schema = schema or (supabase.probe_leads_schema() if supabase.enabled() else {
        "enabled": False,
        "reachable": False,
        "smartlead_tracking_fields": False,
        "error": "Supabase credentials are not configured.",
    })
    current_daily_count = int(counts.get("current_daily_count") or counts.get("existing_today") or 0)
    summary = {
        "day": day,
        "run_id": run_id,
        "started_at": started_at,
        "updated_at": utc_now_iso(),
        "target_emails": target_emails,
        "current_daily_count": current_daily_count,
        "remaining_to_goal": max(target_emails - current_daily_count, 0),
        "pace_status": counts.get("pace_status") or "unknown",
        "cycles_completed": int(counts.get("cycles_completed") or 0),
        "zero_progress_cycles": int(counts.get("zero_progress_cycles") or 0),
        "pending_doc_jobs": db.count_pending_doc_jobs(),
        "doc_backlog": db.summarize_open_doc_jobs(),
        "inventory_total_emails": db.count_kept_emails_for_run(),
        "new_local_emails_found_since_started_at": db.count_new_emails_found_since(started_at),
        "new_creators_discovered_since_started_at": db.count_creators_discovered_since(started_at),
        "results_csv_scope": "inventory_snapshot",
        "latest_seed_batch": latest_seed_batch,
        "seed_history": build_seed_history(db, day)[:10],
        "supabase_schema": effective_schema,
        "paths": paths,
        "counts": dict(counts),
    }
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return str(path)


def build_storage_object_path(day: str, local_path: str) -> str:
    return f"finder-v1/daily/{day}/{Path(local_path).name}"


def upload_output_files(
    supabase: SupabaseClient,
    bucket_name: str,
    day: str,
    paths: Dict[str, str],
    *,
    include_final_artifacts: bool,
) -> Dict[str, str]:
    uploaded: Dict[str, str] = {}
    if not supabase.enabled() or not bucket_name:
        return uploaded
    if not supabase.ensure_storage_bucket(bucket_name, public=False):
        return uploaded
    wanted = ["review", "summary", "seed_report", "new_results"]
    if include_final_artifacts:
        wanted.extend(["final", "audit"])
    for key in wanted:
        local_path = paths.get(key) or ""
        if not local_path:
            continue
        path = Path(local_path)
        if not path.exists():
            continue
        object_path = build_storage_object_path(day, local_path)
        supabase.upload_storage_file(bucket_name, object_path, local_path)
        uploaded[key] = object_path
    return uploaded


def sync_worker_job_status(
    supabase: SupabaseClient,
    job_id: str,
    *,
    name: str,
    schedule: str,
    status: str,
    started_at: str,
    next_run_at: str | None = None,
    duration_ms: int | None = None,
    increment_run_count: bool = False,
) -> None:
    if not supabase.enabled():
        return
    try:
        existing_rows = supabase.fetch_rows("cron_jobs", select="id,run_count", filters=[f"id=eq.{job_id}"], limit=1)
        current_count = int(existing_rows[0]["run_count"]) if existing_rows else 0
        payload = {
            "id": job_id,
            "agent": WORKER_COMPONENT,
            "name": name,
            "schedule": schedule,
            "enabled": True,
            "last_run_at": started_at,
            "next_run_at": next_run_at,
            "last_status": status,
            "last_duration_ms": duration_ms,
            "run_count": current_count + (1 if increment_run_count else 0),
        }
        supabase.upsert_cron_job(payload)
    except Exception as exc:
        print(f"Worker status sync failed for {job_id}: {exc}")


def log_worker_event(
    supabase: SupabaseClient,
    event: str,
    status: str,
    data: Dict[str, Any],
) -> None:
    if not supabase.enabled():
        return
    try:
        supabase.insert_agent_event(
            {
                "agent": WORKER_COMPONENT,
                "event": event,
                "status": status,
                "data": data,
            }
        )
    except Exception as exc:
        print(f"Worker event log failed for {event}: {exc}")


def sync_runtime_heartbeat(
    supabase: SupabaseClient,
    db: FinderDB,
    *,
    day: str,
    run_id: int,
    status: str,
    target_emails: int,
    counts: Counter,
    started_at: str,
    last_progress_at: str | None,
    latest_seed_batch: List[str],
    output_paths: Dict[str, str] | None = None,
) -> None:
    if not supabase.enabled():
        return
    try:
        supabase.upsert_app_setting(
            RUNTIME_STATUS_APP_SETTING_KEY,
            {
                "agent": WORKER_COMPONENT,
                "kind": "daily_run",
                "day": day,
                "run_id": run_id,
                "status": status,
                "started_at": started_at,
                "updated_at": utc_now_iso(),
                "last_progress_at": last_progress_at,
                "target_emails": target_emails,
                "current_daily_count": int(counts.get("current_daily_count") or counts.get("existing_today") or 0),
                "pace_status": counts.get("pace_status") or "unknown",
                "cycles_completed": int(counts.get("cycles_completed") or 0),
                "zero_progress_cycles": int(counts.get("zero_progress_cycles") or 0),
                "low_yield_cycles": int(counts.get("low_yield_cycles") or 0),
                "pending_doc_jobs": db.count_pending_doc_jobs(),
                "doc_backlog": db.summarize_open_doc_jobs(),
                "latest_seed_batch": latest_seed_batch,
                "output_paths": output_paths or {},
                "counts": dict(counts),
            },
        )
    except Exception as exc:
        print(f"Runtime heartbeat sync failed: {exc}")


def count_today_qualified_leads(supabase: SupabaseClient, day: str) -> int:
    if not supabase.enabled():
        return 0
    try:
        rows = supabase.fetch_rows(
            "leads",
            select="id,review_status,sent_to_smartlead,email,reviewed_at,source,status",
            filters=[f"batch_date=eq.{day}", "email=not.is.null", "source=eq.finder_v1"],
        )
    except Exception:
        return 0
    count = 0
    for row in rows:
        status = (row.get("status") or "").strip().lower()
        if status not in {"email_ready", "mgmt_email"}:
            continue
        review_status = row.get("review_status")
        sent_to_smartlead = bool(row.get("sent_to_smartlead"))
        reviewed_at = row.get("reviewed_at")
        if reviewed_at and (sent_to_smartlead or review_status in {"va_approved", "approved", "exported_pending_confirmation"}):
            count += 1
    return count


def load_top_up_request(supabase: SupabaseClient) -> Dict[str, Any] | None:
    if not supabase.enabled():
        return None
    setting = supabase.get_app_setting(TOP_UP_APP_SETTING_KEY)
    if not setting:
        return None
    value = setting.get("value")
    if isinstance(value, dict):
        return value
    return None


def save_top_up_request(supabase: SupabaseClient, payload: Dict[str, Any]) -> None:
    if not supabase.enabled():
        return
    supabase.upsert_app_setting(TOP_UP_APP_SETTING_KEY, payload)


def process_manual_top_up_request() -> Dict[str, Any] | None:
    ensure_dirs()
    cfg = api_config()
    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
    if not supabase.enabled():
        return None

    scrub_invalid_reviewable_emails(supabase)

    request = load_top_up_request(supabase)
    if not request or request.get("status") not in {"requested", "running"}:
        return None

    day = today_business_date()
    request_day = str(request.get("day") or day)
    target_qualified = int(request.get("target_qualified") or DEFAULT_QUALIFIED_TARGET)
    today_qualified = count_today_qualified_leads(supabase, request_day)
    today_email_count = supabase.count_today_net_new_emails(request_day)
    shortfall = max(target_qualified - today_qualified, 0)
    request["today_qualified_count"] = today_qualified
    request["today_email_count"] = today_email_count

    if request_day != day:
        request["status"] = "failed"
        request["failed_at"] = utc_now_iso()
        request["latest_message"] = f"Top-up request was for {request_day}, but today is {day}."
        save_top_up_request(supabase, request)
        log_worker_event(
            supabase,
            "top_up_failed",
            "error",
            {
                "day": day,
                "request_day": request_day,
                "today_qualified_count": today_qualified,
                "target_qualified": target_qualified,
                "message": request["latest_message"],
            },
        )
        return request

    if shortfall <= 0:
        request["status"] = "completed"
        request["completed_at"] = utc_now_iso()
        request["latest_message"] = "Today already has enough qualified leads."
        save_top_up_request(supabase, request)
        log_worker_event(
            supabase,
            "top_up_completed",
            "ok",
            {
                "day": day,
                "today_qualified_count": today_qualified,
                "target_qualified": target_qualified,
                "today_email_count": today_email_count,
                "message": request["latest_message"],
            },
        )
        return request

    requested_target_email_count = int(request.get("target_email_count") or 0)
    if requested_target_email_count > 0:
        target_email_count = max(requested_target_email_count, today_email_count)
    else:
        target_email_count = today_email_count + max(shortfall, TOP_UP_MINIMUM_EMAIL_BATCH)
    request["target_email_count"] = target_email_count

    lock_metadata = read_lock_metadata(LOCK_PATH)
    if lock_metadata:
        request["status"] = "requested"
        request["latest_message"] = f"Waiting for {lock_metadata.get('command') or 'another worker job'} to finish."
        save_top_up_request(supabase, request)
        return request

    request["status"] = "running"
    request["started_at"] = request.get("started_at") or utc_now_iso()
    request["latest_message"] = (
        f"Worker is looking for {max(target_email_count - today_email_count, 0)} more new emails."
        if target_email_count > today_email_count
        else "Worker is finding more leads for today."
    )
    save_top_up_request(supabase, request)
    log_worker_event(
        supabase,
        "top_up_started",
        "ok",
        {
            "day": day,
            "today_qualified_count": today_qualified,
            "target_qualified": target_qualified,
            "today_email_count": today_email_count,
            "target_email_count": target_email_count,
            "message": request["latest_message"],
        },
    )

    try:
        with pipeline_lock(
            LOCK_PATH,
            "daily-run-top-up",
            {
                "request_id": request.get("request_id"),
                "target_qualified": target_qualified,
                "target_email_count": target_email_count,
            },
        ):
            run_daily(target_email_count, DEFAULT_DAILY_BATCH_SIZE, TOP_UP_DEFAULT_MAX_CYCLES, TOP_UP_DEFAULT_HARD_STOP_HOUR)
        refreshed_qualified = count_today_qualified_leads(supabase, day)
        refreshed_emails = supabase.count_today_net_new_emails(day)
        refreshed_shortfall = max(target_qualified - refreshed_qualified, 0)
        remaining_email_shortfall = max(target_email_count - refreshed_emails, 0)
        request["status"] = "completed"
        request["completed_at"] = utc_now_iso()
        request["today_qualified_count"] = refreshed_qualified
        request["today_email_count"] = refreshed_emails
        request["latest_message"] = (
            f"Run finished. Today still needs {remaining_email_shortfall} more new emails."
            if remaining_email_shortfall > 0
            else (
                "Today has enough qualified leads."
                if refreshed_shortfall <= 0
                else f"Run finished. Review the new leads and top up again if you still need {refreshed_shortfall}."
            )
        )
        save_top_up_request(supabase, request)
        log_worker_event(
            supabase,
            "top_up_completed",
            "ok",
            {
                "day": day,
                "today_qualified_count": refreshed_qualified,
                "target_qualified": target_qualified,
                "today_email_count": refreshed_emails,
                "target_email_count": target_email_count,
                "message": request["latest_message"],
            },
        )
    except PipelineBusyError as exc:
        request["status"] = "requested"
        request["latest_message"] = str(exc)
        save_top_up_request(supabase, request)
    except Exception as exc:
        request["status"] = "failed"
        request["failed_at"] = utc_now_iso()
        request["latest_message"] = str(exc)
        save_top_up_request(supabase, request)
        log_worker_event(
            supabase,
            "top_up_failed",
            "error",
            {
                "day": day,
                "today_qualified_count": today_qualified,
                "target_qualified": target_qualified,
                "today_email_count": today_email_count,
                "target_email_count": target_email_count,
                "message": str(exc),
            },
        )
    return request


def pace_status(
    current_count: int,
    target_emails: int,
    started_at: str,
    zero_progress_cycles: int,
    last_progress_at: str | None,
    low_yield_cycles: int = 0,
) -> str:
    if target_emails <= 0:
        return "invalid_target"
    if current_count >= target_emails:
        return "complete"
    if zero_progress_cycles >= 3 or low_yield_cycles >= LOW_YIELD_STALL_THRESHOLD:
        return "stalled"
    progress_time = None
    if last_progress_at:
        try:
            progress_time = datetime.fromisoformat(last_progress_at.replace("Z", "+00:00"))
        except ValueError:
            progress_time = None
    if progress_time and datetime.now(timezone.utc) - progress_time > timedelta(minutes=STALL_PROGRESS_MINUTES):
        return "stalled"
    started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    elapsed_hours = max((datetime.now(timezone.utc) - started).total_seconds() / 3600, 0.01)
    pace = current_count / elapsed_hours
    needed = target_emails / max(12.0, elapsed_hours)
    if pace >= needed * 0.85:
        return "on_pace"
    return "behind_pace"


def build_status_payload(db: FinderDB, supabase: SupabaseClient, day: str, bucket_name: str) -> Dict[str, Any]:
    daily = db.get_daily_progress(day)
    latest_run = db.conn.execute(
        "select id, started_at, finished_at, target_emails, summary_json, output_csv, audit_csv from runs order by id desc limit 1"
    ).fetchone()
    schema = supabase.probe_leads_schema() if supabase.enabled() else {
        "enabled": False,
        "reachable": False,
        "smartlead_tracking_fields": False,
        "error": "Supabase credentials are not configured.",
    }
    payload: Dict[str, Any] = {
        "day": day,
        "lock": read_lock_metadata(LOCK_PATH),
        "pending_doc_jobs": db.count_pending_doc_jobs(),
        "seed_usage_today": [dict(row) for row in db.list_seed_usage_for_day(day)],
        "seed_history": build_seed_history(db, day)[:10],
        "supabase_schema": schema,
        "storage": supabase.probe_storage_bucket(bucket_name),
        "worker_jobs": {},
    }
    if supabase.enabled():
        for job_id in (CRON_JOB_DAILY_ID, CRON_JOB_DOC_ID, CRON_JOB_SMARTLEAD_ID):
            try:
                rows = supabase.fetch_rows(
                    "cron_jobs",
                    select="id,name,schedule,last_status,last_run_at,next_run_at,last_duration_ms,run_count",
                    filters=[f"id=eq.{job_id}"],
                    limit=1,
                )
            except Exception:
                rows = []
            if rows:
                payload["worker_jobs"][job_id] = rows[0]
    if daily:
        payload["daily_progress"] = {
            "status": daily["status"],
            "target_emails": daily["target_emails"],
            "current_count": daily["current_count"],
            "cycles_completed": daily["cycles_completed"],
            "zero_progress_cycles": daily["zero_progress_cycles"],
            "started_at": daily["started_at"],
            "updated_at": daily["updated_at"],
            "last_progress_at": daily["last_progress_at"],
            "summary": json.loads(daily["summary_json"] or "{}"),
            "state": json.loads(daily["state_json"] or "{}"),
        }
    if latest_run:
        summary_path = str(OUTPUT_DIR / f"daily_{day}_summary.json")
        seed_report_path = str(OUTPUT_DIR / f"daily_{day}_seed_report.csv")
        review_path = str(OUTPUT_DIR / f"run_{latest_run['id']}_review.csv")
        new_results_path = str(OUTPUT_DIR / f"run_{latest_run['id']}_new_results.csv")
        payload["latest_run"] = {
            "id": latest_run["id"],
            "started_at": latest_run["started_at"],
            "finished_at": latest_run["finished_at"],
            "target_emails": latest_run["target_emails"],
            "summary": json.loads(latest_run["summary_json"] or "{}"),
            "output_csv": latest_run["output_csv"] or "",
            "audit_csv": latest_run["audit_csv"] or "",
            "storage_objects": {
                "final": build_storage_object_path(day, latest_run["output_csv"]) if latest_run["output_csv"] else "",
                "audit": build_storage_object_path(day, latest_run["audit_csv"]) if latest_run["audit_csv"] else "",
                "review": build_storage_object_path(day, review_path),
                "new_results": build_storage_object_path(day, new_results_path),
                "summary": build_storage_object_path(day, summary_path),
                "seed_report": build_storage_object_path(day, seed_report_path),
            },
        }
    return payload


def run_status_command(day: str) -> None:
    ensure_dirs()
    cfg = api_config()
    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
    db = FinderDB(DB_PATH)
    db.init()
    print(json.dumps(build_status_payload(db, supabase, day, cfg["finder_output_bucket"]), indent=2))


def run_doctor() -> None:
    ensure_dirs()
    cfg = api_config()
    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
    db = FinderDB(DB_PATH)
    db.init()
    schema = supabase.probe_leads_schema() if supabase.enabled() else {
        "enabled": False,
        "reachable": False,
        "smartlead_tracking_fields": False,
        "error": "Supabase credentials are not configured.",
    }
    storage = supabase.probe_storage_bucket(cfg["finder_output_bucket"])
    env = {
        "apify_token": bool(cfg["apify_token"]),
        "youtube_key": bool(cfg["youtube_key"]),
        "smartlead_key": bool(cfg["smartlead_api_key"]),
        "supabase": supabase.enabled(),
        "finder_timezone": cfg["finder_timezone"],
        "finder_output_bucket": cfg["finder_output_bucket"],
    }
    local_state = {
        "db_path": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
        "lock_path": str(LOCK_PATH),
        "pending_doc_jobs": db.count_pending_doc_jobs(),
        "daily_progress_rows": db.conn.execute("select count(*) as count from daily_progress").fetchone()["count"],
        "seed_performance_rows": db.conn.execute("select count(*) as count from seed_performance").fetchone()["count"],
        "unfinished_runs": db.conn.execute("select count(*) as count from runs where finished_at is null").fetchone()["count"],
        "lock": read_lock_metadata(LOCK_PATH),
    }
    latest_daily = db.conn.execute(
        "select day,status,updated_at,target_emails,current_count from daily_progress order by day desc limit 1"
    ).fetchone()
    action_items: List[str] = []
    if not env["apify_token"]:
        action_items.append("Add APIFY_API_TOKEN before cloud deployment.")
    if not env["youtube_key"]:
        action_items.append("Add YOUTUBE_API_KEY before cloud deployment.")
    if not env["supabase"]:
        action_items.append("Add SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY before cloud deployment.")
    migrations_dir = BASE_DIR.parent / "supabase" / "migrations"
    if schema.get("enabled") and not schema.get("smartlead_tracking_fields"):
        action_items.append(f"Apply {migrations_dir / '003_smartlead_tracking.sql'} in the Supabase SQL editor.")
    if schema.get("enabled") and not schema.get("review_app_fields"):
        action_items.append(f"Apply {migrations_dir / '004_finder_review_app.sql'} in the Supabase SQL editor.")
    if schema.get("enabled") and not schema.get("seed_expansion_table"):
        action_items.append(f"Apply {migrations_dir / '006_discovery_seed_expansions.sql'} in the Supabase SQL editor.")
    if env["finder_output_bucket"] and storage.get("enabled") and not storage.get("bucket_exists"):
        action_items.append("Create the Supabase Storage bucket named by FINDER_OUTPUT_BUCKET before cloud deployment, or let the deployment script create it.")
    if not env["finder_output_bucket"]:
        action_items.append("Set FINDER_OUTPUT_BUCKET before cloud deployment so daily files are uploaded to the cloud.")
    if not env["smartlead_key"]:
        action_items.append("Add SMARTLEAD_API_KEY before enabling automatic Smartlead reconciliation.")
    if read_lock_metadata(LOCK_PATH):
        action_items.append("A pipeline lock is currently present. Make sure no old daily-run process is still active before cloud deployment.")
    if int(local_state["unfinished_runs"] or 0) > 0 and not read_lock_metadata(LOCK_PATH):
        action_items.append("There are unfinished local run records from earlier interruptions. Review them before cloud deployment so the overseer starts from clean state.")
    if latest_daily and latest_daily["status"] == "running" and not read_lock_metadata(LOCK_PATH):
        action_items.append("Daily progress still says running even though no lock is active. That usually means an earlier run ended unexpectedly.")
    readiness = {
        "cloud_ready": bool(
            env["apify_token"]
            and env["youtube_key"]
            and env["supabase"]
            and schema.get("reachable")
            and schema.get("smartlead_tracking_fields")
            and schema.get("seed_expansion_table")
            and bool(env["finder_output_bucket"])
            and storage.get("bucket_exists")
        ),
        "overseer_ready": bool(
            env["apify_token"]
            and env["youtube_key"]
            and env["supabase"]
            and schema.get("reachable")
            and schema.get("smartlead_tracking_fields")
            and schema.get("seed_expansion_table")
            and bool(env["finder_output_bucket"])
            and storage.get("bucket_exists")
        ),
        "schema_ready": bool(schema.get("smartlead_tracking_fields") and schema.get("seed_expansion_table")),
        "review_app_ready": bool(
            env["smartlead_key"]
            and schema.get("smartlead_tracking_fields")
            and schema.get("review_app_fields")
        ),
    }
    print(json.dumps({
        "environment": env,
        "local_state": local_state,
        "supabase_schema": schema,
        "storage": storage,
        "readiness": readiness,
        "action_items": action_items,
    }, indent=2))


def run_repair_state() -> None:
    ensure_dirs()
    if read_lock_metadata(LOCK_PATH):
        raise RuntimeError("Cannot repair state while the pipeline lock is active.")
    db = FinderDB(DB_PATH)
    db.init()
    now = utc_now_iso()
    unfinished = db.conn.execute(
        "select id, summary_json, output_csv, audit_csv from runs where finished_at is null order by id asc"
    ).fetchall()
    repaired_runs = 0
    for row in unfinished:
        db.conn.execute(
            """
            update runs
            set finished_at = ?, summary_json = coalesce(summary_json, ?)
            where id = ?
            """,
            (now, row["summary_json"] or "{}", row["id"]),
        )
        repaired_runs += 1

    repaired_days = 0
    running_days = db.conn.execute(
        "select day, summary_json from daily_progress where status = 'running' order by day asc"
    ).fetchall()
    for row in running_days:
        summary = json.loads(row["summary_json"] or "{}")
        next_status = "stalled" if summary.get("pace_status") == "stalled" else "partial"
        db.conn.execute(
            "update daily_progress set status = ?, updated_at = ? where day = ?",
            (next_status, now, row["day"]),
        )
        repaired_days += 1
    db.conn.commit()
    print(json.dumps({
        "repaired_runs": repaired_runs,
        "repaired_days": repaired_days,
        "repaired_at": now,
    }, indent=2))


def run_smartlead_reconcile_command(limit: int, historical: bool) -> None:
    ensure_dirs()
    cfg = api_config()
    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
    scrub_result = scrub_invalid_reviewable_emails(supabase) if not historical else {"cleaned": 0, "by_kind": {}, "examples": []}
    started_at = utc_now_iso()
    sync_worker_job_status(
        supabase,
        CRON_JOB_SMARTLEAD_ID,
        name="Finder V1 Smartlead Reconcile",
        schedule=SMARTLEAD_RECONCILE_SCHEDULE,
        status="running",
        started_at=started_at,
        next_run_at=next_interval_iso(10),
        increment_run_count=not historical,
    )
    outcome = "success"
    result: Dict[str, Any] = {"historical": historical, "summary": {}}
    if scrub_result.get("cleaned"):
        result["invalid_email_scrub"] = scrub_result
    try:
        result = reconcile_smartlead(
            supabase,
            cfg["smartlead_api_key"],
            limit=limit,
            historical=historical,
        )
        if scrub_result.get("cleaned"):
            result["invalid_email_scrub"] = scrub_result
        if result.get("rate_limited"):
            outcome = "partial"
    except Exception:
        outcome = "error"
        raise
    finally:
        sync_worker_job_status(
            supabase,
            CRON_JOB_SMARTLEAD_ID,
            name="Finder V1 Smartlead Reconcile",
            schedule=SMARTLEAD_RECONCILE_SCHEDULE,
            status=outcome,
            started_at=started_at,
            next_run_at=next_interval_iso(10),
        )
        log_worker_event(
            supabase,
            "smartlead_backfill_completed" if historical and outcome == "success" else (
                "smartlead_backfill_partial" if historical and outcome == "partial" else (
                "smartlead_reconcile_completed" if outcome == "success" else "smartlead_reconcile_failed"
                )
            ),
            "warning" if outcome == "partial" else ("ok" if outcome == "success" else "error"),
            compact_smartlead_reconcile_event_data(result, limit=limit, historical=historical),
        )
    top_up_result = None
    if not historical:
        try:
            top_up_result = process_manual_top_up_request()
        except Exception as exc:
            print(f"Top-up request processing failed: {exc}")
    if top_up_result:
        result["top_up"] = top_up_result
    print(json.dumps(result, indent=2))


def run_seed_cycle(
    db: FinderDB,
    counts: Counter,
    apify: ApifyHTTPClient,
    youtube_key: str,
    supabase: SupabaseClient,
    duplicate_guard: Dict[str, Any] | None,
    run_id: int,
    seed: str,
    target_emails: int,
    started_at: str,
    batch_date: str | None = None,
) -> Dict[str, int]:
    cycle = Counter()
    if counts["existing_today"] + counts["emails_kept"] >= target_emails:
        return dict(cycle)
    print(f"\nDiscovering handles from @{seed}...")
    handles = discover_handles(apify, seed)
    counts["discovered"] += len(handles)
    cycle["discovered"] += len(handles)
    if supabase.enabled():
        handles, skipped_existing_live_pre_enrich = filter_existing_live_handles_before_enrich(supabase, handles)
        counts["skipped_existing_live_pre_enrich"] += skipped_existing_live_pre_enrich
        cycle["skipped_existing_live"] += skipped_existing_live_pre_enrich
        if skipped_existing_live_pre_enrich:
            print(f"  Skipped {skipped_existing_live_pre_enrich} handle(s) before enrichment because they already exist live")
    new_handles = []
    for handle in handles:
        if not db.creator_exists(handle):
            db.upsert_creator(handle, seed)
            new_handles.append(handle)
    print(f"  Found {len(handles)} handles, {len(new_handles)} new")

    missing_profiles = discovered_without_profile(db, seed)
    to_enrich = sorted(set(new_handles + missing_profiles))
    profiles = enrich_handles(apify, to_enrich)
    print(f"  Enriched {len(profiles)} profiles")
    counts["enriched"] += len(profiles)
    cycle["enriched"] += len(profiles)

    cached_profiles = unfinished_cached_profiles(db, seed)
    if cached_profiles:
        print(f"  Reusing {len(cached_profiles)} cached profiles that still need work")

    for profile in profiles:
        handle = profile["username"]
        followers = int(profile.get("followers") or 0)
        db.set_profile(handle, profile, followers)

    new_usernames = {prof["username"] for prof in profiles}
    queue = profiles + [p for p in cached_profiles if p.get("username") not in new_usernames]
    if supabase.enabled():
        queue, skipped_existing_live = filter_existing_live_email_profiles(supabase, queue)
        counts["skipped_existing_live_email"] += skipped_existing_live
        cycle["skipped_existing_live"] += skipped_existing_live
        if skipped_existing_live:
            print(f"  Skipped {skipped_existing_live} profile(s) already in live systems")

    cycle_baseline = Counter(counts)

    for profile in queue:
        handle = profile["username"]
        cycle["profiles_checked"] += 1
        try:
            process_profile(db, counts, profile, apify, youtube_key, supabase, duplicate_guard, seed, batch_date=batch_date)
            checkpoint_outputs_with_review(db, run_id, counts, started_at)
            if counts["existing_today"] + counts["emails_kept"] >= target_emails:
                break
        except Exception as exc:
            counts["profile_errors"] += 1
            db.record_contact_result(handle, None, None, None, f"Profile processing failed: {exc}")
            print(f"  Skipped @{handle}: {exc}")
            checkpoint_outputs_with_review(db, run_id, counts, started_at)

    harvested_after = check_doc_jobs(
        db,
        counts,
        apify,
        supabase,
        duplicate_guard,
        limit=DEFAULT_DOC_CHECK_LIMIT,
        time_budget_seconds=DEFAULT_DOC_CHECK_TIME_BUDGET_SECONDS,
        batch_date=batch_date,
    )
    if harvested_after:
        cycle["doc_emails_harvested"] += harvested_after
        print(f"  Harvested {harvested_after} DOC email(s) after processing @{seed}")

    cycle["qualified"] = counts["qualified"] - cycle_baseline["qualified"]
    cycle["kept"] = counts["emails_kept"] - cycle_baseline["emails_kept"]
    cycle["duplicate_emails"] = (
        counts["duplicate_existing_smartlead"]
        + counts["duplicate_existing_live_email"]
        - cycle_baseline["duplicate_existing_smartlead"]
        - cycle_baseline["duplicate_existing_live_email"]
    )
    cycle["doc_jobs_submitted"] = counts["doc_jobs_submitted"] - cycle_baseline["doc_jobs_submitted"]
    cycle["doc_jobs_reused"] = counts["doc_jobs_reused"] - cycle_baseline["doc_jobs_reused"]
    return dict(cycle)


def process_profile(
    db: FinderDB,
    counts: Counter,
    profile: Dict[str, Any],
    apify: ApifyHTTPClient,
    youtube_key: str,
    supabase: SupabaseClient,
    duplicate_guard: Dict[str, Any] | None,
    source_seed: str,
    *,
    batch_date: str | None = None,
) -> bool:
    handle = profile["username"]
    followers = int(profile.get("followers") or 0)
    existing = db.get_creator(handle)
    if existing and existing["best_email"]:
        return False
    if followers < MIN_FOLLOWERS:
        db.set_qualification(handle, False, "under_100k", 1.0, "Profile is below the follower threshold.", "threshold")
        counts["under_threshold"] += 1
        return False

    result = deterministic_qualification(profile)

    db.set_qualification(
        handle,
        bool(result.get("qualified")),
        result.get("reject_reason"),
        float(result.get("confidence") or 0.5),
        result.get("why") or "",
        QUALIFICATION_VERSION,
    )

    if not result.get("qualified"):
        counts[f"reject_{result.get('reject_reason') or 'other'}"] += 1
        return False
    counts["qualified"] += 1

    email_candidates = deep_email_search(profile)
    for candidate in email_candidates:
        keep = is_reviewable_email_type(candidate["email_type"])
        db.add_email_candidate(
            handle,
            candidate["email"],
            candidate["email_type"],
            candidate["source_method"],
            candidate["source_url"],
            candidate["source_path"],
            keep,
            candidate["notes"],
        )
    usable_email_candidates = filter_duplicate_email_candidates(supabase, duplicate_guard, counts, handle, email_candidates)
    best = best_candidate(usable_email_candidates)

    if apify.enabled():
        if not profile.get("youtube_url"):
            youtube_urls = discover_youtube_urls(profile)
            if youtube_urls:
                profile["youtube_url"] = youtube_urls[0]
                db.set_profile(handle, profile, followers)
        if not profile.get("youtube_url") and youtube_key:
            matched_youtube = search_channel(profile, youtube_key)
            if matched_youtube:
                profile["youtube_url"] = matched_youtube
                db.set_profile(handle, profile, followers)
                if supabase.enabled():
                    try:
                        sync_youtube_state_to_supabase(supabase, profile, source_seed, batch_date=batch_date)
                    except Exception as exc:
                        counts["supabase_sync_errors"] += 1
                        print(f"  Supabase YouTube sync failed for @{handle}: {exc}")
        try:
            job, youtube_candidates, is_new_job = submit_doc_job(apify, db, handle, profile.get("youtube_url") or "")
        except Exception as exc:
            counts["doc_submit_errors"] += 1
            counts[f"doc_submit_error_{normalize_counter_key(type(exc).__name__)}"] += 1
            if "403" in str(exc):
                counts["doc_submit_forbidden"] += 1
            youtube_candidates = []
            db.record_youtube_fallback(handle, f"failed: {exc}")
        else:
            if job:
                status = "submitted" if is_new_job else "pending"
                db.record_youtube_fallback(handle, f"{status}:{job['apify_run_id']}")
                counts["doc_jobs_submitted" if is_new_job else "doc_jobs_reused"] += 1
                if supabase.enabled():
                    try:
                        sync_youtube_state_to_supabase(supabase, profile, source_seed, batch_date=batch_date)
                    except Exception as exc:
                        counts["supabase_sync_errors"] += 1
                        print(f"  Supabase YouTube sync failed for @{handle}: {exc}")
        for candidate in youtube_candidates:
            keep = is_reviewable_email_type(candidate["email_type"])
            db.add_email_candidate(
                handle,
                candidate["email"],
                candidate["email_type"],
                candidate["source_method"],
                candidate["source_url"],
                candidate["source_path"],
                keep,
                candidate["notes"],
            )
        usable_youtube_candidates = filter_duplicate_email_candidates(supabase, duplicate_guard, counts, handle, youtube_candidates)
        youtube_best = best_candidate(usable_youtube_candidates)
        if youtube_best:
            best = best_candidate(usable_email_candidates + usable_youtube_candidates) or youtube_best

    if best:
        db.record_contact_result(handle, best["email"], best["email_type"], best["source_method"], best["notes"])
        remember_session_email(duplicate_guard, handle, best["email"])
        counts["emails_kept"] += 1
        print(f"  Kept {best['email']} for @{handle} ({best['email_type']})")
        if supabase.enabled():
            sync_best_to_supabase(supabase, counts, handle, followers, profile, best, source_seed, batch_date=batch_date)
        return True

    db.record_contact_result(handle, None, None, None, "No kept email found.")
    counts["no_email"] += 1
    return False


def run_pipeline(seeds: List[str], target_emails: int) -> None:
    ensure_dirs()
    target_emails = validate_target_emails(target_emails, command_name="run")
    cfg = api_config()
    apify = ApifyHTTPClient(cfg["apify_token"])
    youtube_key = cfg["youtube_key"]
    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
    duplicate_guard = build_duplicate_guard(cfg["smartlead_api_key"])
    db = FinderDB(DB_PATH)
    db.init()
    run_id = db.create_run(seeds, target_emails)
    started_at = db.conn.execute("select started_at from runs where id = ?", (run_id,)).fetchone()["started_at"]
    run_day = today_business_date()

    counts = Counter()
    paths = {"final": "", "audit": "", "review": "", "new_results": ""}
    if supabase.enabled():
        todays_existing = supabase.count_today_net_new_emails(run_day)
        counts["existing_today"] = todays_existing
        print(f"Existing centralized emails for today: {todays_existing}")
    print(f"Run {run_id}: targeting {target_emails} usable emails")
    try:
        if apify.enabled():
            harvested = check_doc_jobs(
                db,
                counts,
                apify,
                supabase,
                duplicate_guard,
                limit=DEFAULT_DOC_PREFLIGHT_LIMIT,
                time_budget_seconds=DEFAULT_DOC_PREFLIGHT_TIME_BUDGET_SECONDS,
                batch_date=run_day,
            )
            if harvested:
                print(f"Harvested {harvested} email(s) from pending DOC jobs before starting new work.")
        if supabase.enabled() and counts["existing_today"] < target_emails:
            print("\nWorking existing Supabase inventory first...")
            inventory_profiles = []
            for row in supabase.fetch_youtube_only_without_email():
                profile = supabase_row_to_profile(row)
                db.upsert_creator(profile["username"], "supabase_inventory")
                db.set_profile(profile["username"], profile, int(profile.get("followers") or 0))
                inventory_profiles.append(profile)
            print(f"  Loaded {len(inventory_profiles)} youtube_only leads from Supabase")

            for row in supabase.fetch_no_email_without_youtube():
                profile = supabase_row_to_profile(row)
                db.upsert_creator(profile["username"], "supabase_inventory")
                db.set_profile(profile["username"], profile, int(profile.get("followers") or 0))
                inventory_profiles.append(profile)
            print(f"  Loaded {len(inventory_profiles)} total no-email inventory leads")

            seen_inventory = set()
            for profile in inventory_profiles:
                handle = profile["username"]
                if handle in seen_inventory:
                    continue
                seen_inventory.add(handle)
                try:
                    process_profile(db, counts, profile, apify, youtube_key, supabase, duplicate_guard, "supabase_inventory", batch_date=run_day)
                    checkpoint_outputs_with_review(db, run_id, counts, started_at)
                except Exception as exc:
                    counts["profile_errors"] += 1
                    db.record_contact_result(handle, None, None, None, f"Inventory profile failed: {exc}")
                    checkpoint_outputs_with_review(db, run_id, counts, started_at)
                    print(f"  Skipped inventory @{handle}: {exc}")
                if counts["existing_today"] + counts["emails_kept"] >= target_emails:
                    break

        for seed in seeds:
            if counts["existing_today"] + counts["emails_kept"] >= target_emails:
                break
            print(f"\nDiscovering handles from @{seed}...")
            handles = discover_handles(apify, seed)
            counts["discovered"] += len(handles)
            if supabase.enabled():
                handles, skipped_existing_live_pre_enrich = filter_existing_live_handles_before_enrich(supabase, handles)
                counts["skipped_existing_live_pre_enrich"] += skipped_existing_live_pre_enrich
                if skipped_existing_live_pre_enrich:
                    print(f"  Skipped {skipped_existing_live_pre_enrich} handle(s) before enrichment because they already exist live")
            new_handles = []
            for handle in handles:
                if not db.creator_exists(handle):
                    db.upsert_creator(handle, seed)
                    new_handles.append(handle)
            print(f"  Found {len(handles)} handles, {len(new_handles)} new")

            missing_profiles = discovered_without_profile(db, seed)
            to_enrich = sorted(set(new_handles + missing_profiles))
            profiles = enrich_handles(apify, to_enrich)
            print(f"  Enriched {len(profiles)} profiles")
            counts["enriched"] += len(profiles)

            cached_profiles = unfinished_cached_profiles(db, seed)
            if cached_profiles:
                print(f"  Reusing {len(cached_profiles)} cached profiles that still need work")

            for profile in profiles:
                handle = profile["username"]
                followers = int(profile.get("followers") or 0)
                db.set_profile(handle, profile, followers)

            new_usernames = {prof["username"] for prof in profiles}
            queue = profiles + [p for p in cached_profiles if p.get("username") not in new_usernames]
            if supabase.enabled():
                queue, skipped_existing_live = filter_existing_live_email_profiles(supabase, queue)
                counts["skipped_existing_live_email"] += skipped_existing_live
                if skipped_existing_live:
                    print(f"  Skipped {skipped_existing_live} profile(s) already holding a live Supabase email")

            for profile in queue:
                handle = profile["username"]
                try:
                    process_profile(db, counts, profile, apify, youtube_key, supabase, duplicate_guard, seed, batch_date=run_day)
                    checkpoint_outputs_with_review(db, run_id, counts, started_at)

                    if counts["existing_today"] + counts["emails_kept"] >= target_emails:
                        break
                except Exception as exc:
                    counts["profile_errors"] += 1
                    db.record_contact_result(handle, None, None, None, f"Profile processing failed: {exc}")
                    print(f"  Skipped @{handle}: {exc}")
                    checkpoint_outputs_with_review(db, run_id, counts, started_at)

            if counts["existing_today"] + counts["emails_kept"] >= target_emails:
                break
    finally:
        if apify.enabled():
            check_doc_jobs(
                db,
                counts,
                apify,
                supabase,
                duplicate_guard,
                limit=DEFAULT_DOC_PREFLIGHT_LIMIT,
                time_budget_seconds=DEFAULT_DOC_PREFLIGHT_TIME_BUDGET_SECONDS,
                batch_date=run_day,
            )
        paths = checkpoint_outputs_with_review(db, run_id, counts, started_at)
        summary = dict(counts)
        db.finish_run(run_id, summary, paths["final"], paths["audit"])
        print("\nRun complete.")
        print(json.dumps(summary, indent=2))
        print(f"Results CSV: {paths['final']}")
        print(f"Audit CSV: {paths['audit']}")
        print(f"New results CSV: {paths.get('new_results', '')}")
        print(f"Review CSV: {paths['review']}")


def utc_now_fallback() -> str:
    return utc_now_iso()


def run_daily(target_emails: int, seed_batch_size: int, max_cycles: int, hard_stop_hour_local: int) -> None:
    ensure_dirs()
    target_emails = validate_target_emails(target_emails, command_name="daily-run")
    cfg = api_config()
    apify = ApifyHTTPClient(cfg["apify_token"])
    youtube_key = cfg["youtube_key"]
    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
    duplicate_guard = build_duplicate_guard(cfg["smartlead_api_key"])
    bucket_name = cfg["finder_output_bucket"]
    supabase_schema = supabase.probe_leads_schema() if supabase.enabled() else {
        "enabled": False,
        "reachable": False,
        "smartlead_tracking_fields": False,
        "error": "Supabase credentials are not configured.",
    }
    db = FinderDB(DB_PATH)
    db.init()

    scrub_invalid_reviewable_emails(supabase)

    day = today_business_date()
    existing_progress = db.get_daily_progress(day)
    started_at = existing_progress["started_at"] if existing_progress else utc_now_iso()
    seeds = []
    if existing_progress and existing_progress["state_json"]:
        try:
            saved_state = json.loads(existing_progress["state_json"])
            seeds = saved_state.get("last_seed_batch") or []
        except Exception:
            seeds = []
    run_id = db.create_run(seeds, target_emails)
    counts = Counter()
    counts["existing_today"] = supabase.count_today_net_new_emails(day) if supabase.enabled() else 0
    current_count = counts["existing_today"]
    saved_summary: Dict[str, Any] = {}
    if existing_progress and existing_progress["summary_json"]:
        try:
            saved_summary = json.loads(existing_progress["summary_json"])
        except Exception:
            saved_summary = {}
    zero_progress_cycles = int(existing_progress["zero_progress_cycles"] or 0) if existing_progress else 0
    cycles_completed = int(existing_progress["cycles_completed"] or 0) if existing_progress else 0
    last_progress_at = existing_progress["last_progress_at"] if existing_progress else None
    low_yield_cycles = int(saved_summary.get("low_yield_cycles") or 0)
    state = {"last_seed_batch": seeds, "day": day}
    sync_worker_job_status(
        supabase,
        CRON_JOB_DAILY_ID,
        name="Finder V1 Daily Run",
        schedule=DAILY_RUN_SCHEDULE,
        status="running",
        started_at=started_at,
        next_run_at=next_business_time_iso(2, 0),
        increment_run_count=not bool(existing_progress),
    )
    log_worker_event(
        supabase,
        "daily_run_started",
        "ok",
        {
            "day": day,
            "run_id": run_id,
            "current_daily_count": current_count,
            "target_emails": target_emails,
            "cycles_completed": cycles_completed,
            "zero_progress_cycles": zero_progress_cycles,
            "pending_doc_jobs": db.count_pending_doc_jobs(),
        },
    )
    counts["current_daily_count"] = current_count
    counts["cycles_completed"] = cycles_completed
    counts["zero_progress_cycles"] = zero_progress_cycles
    counts["low_yield_cycles"] = low_yield_cycles
    db.upsert_daily_progress(
        day,
        target_emails=target_emails,
        status="running",
        current_count=current_count,
        cycles_completed=cycles_completed,
        zero_progress_cycles=zero_progress_cycles,
        summary=dict(counts),
        state=state,
        started_at=started_at,
        last_progress_at=last_progress_at,
    )
    sync_runtime_heartbeat(
        supabase,
        db,
        day=day,
        run_id=run_id,
        status="running",
        target_emails=target_emails,
        counts=counts,
        started_at=started_at,
        last_progress_at=last_progress_at,
        latest_seed_batch=state.get("last_seed_batch") or [],
    )
    print(f"Daily run {run_id}: {current_count}/{target_emails} emails credited for {day}")
    daily_paths = {"final": "", "audit": "", "review": "", "summary": "", "seed_report": "", "new_results": ""}
    fatal_error: Exception | None = None
    try:
        if apify.enabled() and current_count < target_emails:
            harvested = check_doc_jobs(
                db,
                counts,
                apify,
                supabase,
                duplicate_guard,
                limit=DEFAULT_DOC_CHECK_LIMIT,
                time_budget_seconds=DEFAULT_DOC_CHECK_TIME_BUDGET_SECONDS,
                batch_date=day,
            )
            if harvested:
                current_count = (supabase.count_today_net_new_emails(day) if supabase.enabled() else db.count_kept_emails_since(started_at))
                last_progress_at = utc_now_iso()

        while cycles_completed < max_cycles:
            current_count = supabase.count_today_net_new_emails(day) if supabase.enabled() else db.count_kept_emails_since(started_at)
            if current_count >= target_emails:
                break
            if daily_hard_stop_reached(day, hard_stop_hour_local):
                counts["hard_stop_reached"] += 1
                print(f"Stopping daily run because hard stop hour local {hard_stop_hour_local} was reached.")
                break

            seed_batch = select_seed_batch(supabase, seed_batch_size)
            if not seed_batch:
                print("No unexpanded human-qualified Supabase seeds available.")
                counts["no_unexpanded_qualified_seeds"] += 1
                break

            cycles_completed += 1
            state["last_seed_batch"] = seed_batch
            print(f"\nCycle {cycles_completed}: trying {len(seed_batch)} seeds -> {', '.join('@' + seed for seed in seed_batch)}")
            cycle_start_count = current_count
            cycle_doc_harvest = 0
            for seed in seed_batch:
                if daily_hard_stop_reached(day, hard_stop_hour_local):
                    counts["hard_stop_reached"] += 1
                    print(f"Stopping daily run before @{seed} because hard stop hour local {hard_stop_hour_local} was reached.")
                    break
                before_cycle = Counter(counts)
                stats = run_seed_cycle(
                    db,
                    counts,
                    apify,
                    youtube_key,
                    supabase,
                    duplicate_guard,
                    run_id,
                    seed,
                    target_emails,
                    started_at,
                    batch_date=day,
                )
                if supabase.enabled():
                    supabase.record_seed_expansion(
                        seed,
                        related_found=int(stats.get("discovered") or 0),
                        profiles_checked=int(stats.get("profiles_checked") or stats.get("enriched") or 0),
                        leads_saved=int(stats.get("kept") or 0),
                        doc_jobs_submitted=int(stats.get("doc_jobs_submitted") or 0),
                    )
                    counts["seed_expansions_recorded"] += 1
                current_count = supabase.count_today_net_new_emails(day) if supabase.enabled() else db.count_kept_emails_since(started_at)
                cycle_doc_harvest += int(stats.get("doc_emails_harvested") or 0)
                db.record_seed_usage(
                    day,
                    seed,
                    cycle_index=cycles_completed,
                    discovered=int(stats.get("discovered") or 0),
                    enriched=int(stats.get("enriched") or 0),
                    qualified=int(stats.get("qualified") or 0),
                    kept=int(stats.get("kept") or 0),
                    skipped_existing_live=int(stats.get("skipped_existing_live") or 0),
                    duplicate_emails=int(stats.get("duplicate_emails") or 0),
                    doc_submitted=int(stats.get("doc_jobs_submitted") or 0),
                    doc_harvested=int(stats.get("doc_emails_harvested") or 0),
                    outcome="progress" if current_count > cycle_start_count else "no_progress",
                )
                db.update_seed_performance(
                    seed,
                    discovered=int(stats.get("discovered") or 0),
                    enriched=int(stats.get("enriched") or 0),
                    qualified=int(stats.get("qualified") or 0),
                    kept=int(stats.get("kept") or 0),
                    skipped_existing_live=int(stats.get("skipped_existing_live") or 0),
                    duplicate_emails=int(stats.get("duplicate_emails") or 0),
                    doc_submitted=int(stats.get("doc_jobs_submitted") or 0),
                    doc_harvested=int(stats.get("doc_emails_harvested") or 0),
                )
                if current_count >= target_emails:
                    break
                if before_cycle == counts and not stats:
                    continue

            cycle_gain = current_count - cycle_start_count
            if cycle_gain > 0:
                zero_progress_cycles = 0
                last_progress_at = utc_now_iso()
            else:
                zero_progress_cycles += 1
            if cycle_gain <= LOW_YIELD_CYCLE_GAIN_THRESHOLD:
                low_yield_cycles += 1
            else:
                low_yield_cycles = 0

            counts["current_daily_count"] = current_count
            counts["cycles_completed"] = cycles_completed
            counts["zero_progress_cycles"] = zero_progress_cycles
            counts["low_yield_cycles"] = low_yield_cycles
            counts["last_cycle_gain"] = cycle_gain
            counts["last_cycle_doc_harvest"] = cycle_doc_harvest
            counts["pace_status"] = pace_status(
                current_count,
                target_emails,
                started_at,
                zero_progress_cycles,
                last_progress_at,
                low_yield_cycles,
            )
            daily_paths = checkpoint_outputs_with_review(db, run_id, counts, started_at)
            daily_paths["seed_report"] = write_daily_seed_report_csv(db, day)
            daily_paths["summary"] = write_daily_summary_json(
                db,
                run_id,
                day,
                started_at,
                target_emails,
                counts,
                seed_batch,
                daily_paths,
                supabase,
                schema=supabase_schema,
            )
            uploaded = upload_output_files(
                supabase,
                bucket_name,
                day,
                daily_paths,
                include_final_artifacts=False,
            )
            if uploaded:
                state["last_uploaded_paths"] = uploaded
            db.upsert_daily_progress(
                day,
                target_emails=target_emails,
                status="running",
                current_count=current_count,
                cycles_completed=cycles_completed,
                zero_progress_cycles=zero_progress_cycles,
                summary=dict(counts),
                state=state,
                started_at=started_at,
                last_progress_at=last_progress_at,
            )
            sync_worker_job_status(
                supabase,
                CRON_JOB_DAILY_ID,
                name="Finder V1 Daily Run",
                schedule=DAILY_RUN_SCHEDULE,
                status="running",
                started_at=started_at,
                next_run_at=next_business_time_iso(2, 0),
            )
            sync_runtime_heartbeat(
                supabase,
                db,
                day=day,
                run_id=run_id,
                status="running",
                target_emails=target_emails,
                counts=counts,
                started_at=started_at,
                last_progress_at=last_progress_at,
                latest_seed_batch=seed_batch,
                output_paths=state.get("last_uploaded_paths") or {},
            )
            if counts["pace_status"] == "stalled":
                log_worker_event(
                    supabase,
                    "daily_run_stalled",
                    "warning",
                    {
                        "day": day,
                        "run_id": run_id,
                        "current_daily_count": current_count,
                        "target_emails": target_emails,
                        "cycles_completed": cycles_completed,
                        "zero_progress_cycles": zero_progress_cycles,
                        "low_yield_cycles": low_yield_cycles,
                        "pending_doc_jobs": db.count_pending_doc_jobs(),
                        "output_paths": state.get("last_uploaded_paths") or {},
                    },
                )
            if zero_progress_cycles >= 5:
                print("Stopping daily run after repeated zero-progress cycles.")
                break
            if low_yield_cycles >= LOW_YIELD_STALL_THRESHOLD:
                print("Stopping daily run after repeated low-yield cycles.")
                break
            if counts["hard_stop_reached"]:
                break
    except Exception as exc:
        fatal_error = exc
        counts["fatal_errors"] += 1
        counts["fatal_error_message"] = str(exc)
    finally:
        try:
            current_count = supabase.count_today_net_new_emails(day) if supabase.enabled() else db.count_kept_emails_since(started_at)
        except Exception:
            current_count = db.count_kept_emails_since(started_at)
        counts["current_daily_count"] = current_count
        counts["cycles_completed"] = cycles_completed
        counts["zero_progress_cycles"] = zero_progress_cycles
        counts["low_yield_cycles"] = low_yield_cycles
        counts["pace_status"] = "error" if fatal_error else pace_status(
            current_count,
            target_emails,
            started_at,
            zero_progress_cycles,
            last_progress_at,
            low_yield_cycles,
        )
        if fatal_error or counts["pace_status"] == "invalid_target":
            status = "error"
        else:
            status = "complete" if current_count >= target_emails else ("stalled" if counts["pace_status"] == "stalled" else "partial")
        try:
            paths = checkpoint_outputs_with_review(db, run_id, counts, started_at)
            paths["seed_report"] = write_daily_seed_report_csv(db, day)
            paths["summary"] = write_daily_summary_json(
                db,
                run_id,
                day,
                started_at,
                target_emails,
                counts,
                state.get("last_seed_batch") or [],
                paths,
                supabase,
                schema=supabase_schema,
            )
            uploaded = upload_output_files(
                supabase,
                bucket_name,
                day,
                paths,
                include_final_artifacts=True,
            )
            if uploaded:
                state["last_uploaded_paths"] = uploaded
        except Exception as final_exc:
            paths = daily_paths
            counts["finalization_errors"] += 1
            counts["finalization_error_message"] = str(final_exc)
            status = "error"
        db.upsert_daily_progress(
            day,
            target_emails=target_emails,
            status=status,
            current_count=current_count,
            cycles_completed=cycles_completed,
            zero_progress_cycles=zero_progress_cycles,
            summary=dict(counts),
            state=state,
            started_at=started_at,
            last_progress_at=last_progress_at,
        )
        db.finish_run(run_id, dict(counts), paths.get("final", ""), paths.get("audit", ""))
        duration_ms = None
        try:
            duration_ms = int(
                (datetime.fromisoformat(utc_now_iso()) - datetime.fromisoformat(started_at.replace("Z", "+00:00"))).total_seconds() * 1000
            )
        except Exception:
            duration_ms = None
        sync_worker_job_status(
            supabase,
            CRON_JOB_DAILY_ID,
            name="Finder V1 Daily Run",
            schedule=DAILY_RUN_SCHEDULE,
            status=status,
            started_at=started_at,
            next_run_at=next_business_time_iso(2, 0),
            duration_ms=duration_ms,
        )
        sync_runtime_heartbeat(
            supabase,
            db,
            day=day,
            run_id=run_id,
            status=status,
            target_emails=target_emails,
            counts=counts,
            started_at=started_at,
            last_progress_at=last_progress_at,
            latest_seed_batch=state.get("last_seed_batch") or [],
            output_paths=state.get("last_uploaded_paths") or {},
        )
        final_event = {
            "complete": "daily_run_completed",
            "stalled": "daily_run_stalled",
            "partial": "daily_run_partial",
            "error": "daily_run_failed",
        }.get(status, "daily_run_partial")
        log_worker_event(
            supabase,
            final_event,
            "warning" if status in {"stalled", "partial"} else ("error" if status == "error" else "ok"),
            {
                "day": day,
                "run_id": run_id,
                "current_daily_count": current_count,
                "target_emails": target_emails,
                "pace_status": counts["pace_status"],
                "cycles_completed": cycles_completed,
                "zero_progress_cycles": zero_progress_cycles,
                "pending_doc_jobs": db.count_pending_doc_jobs(),
                "output_paths": state.get("last_uploaded_paths") or {},
            },
        )

    if fatal_error:
        raise fatal_error

    print("\nDaily run complete.")
    print(json.dumps(dict(counts), indent=2))
    print(f"Results CSV: {paths['final']}")
    print(f"Audit CSV: {paths['audit']}")
    print(f"Review CSV: {paths['review']}")
    print(f"Seed report CSV: {paths['seed_report']}")
    print(f"Summary JSON: {paths['summary']}")


def main() -> None:
    args = parse_args()
    try:
        if args.command == "status":
            run_status_command(args.day)
        elif args.command == "doctor":
            run_doctor()
        elif args.command == "repair-state":
            run_repair_state()
        elif args.command == "reconcile-smartlead":
            run_smartlead_reconcile_command(args.limit, args.historical)
        elif args.command == "run":
            seeds = [s.strip().lower().replace("@", "") for s in args.seeds.split(",") if s.strip()]
            if not seeds:
                raise RuntimeError("Provide at least one seed handle with --seeds.")
            with pipeline_lock(LOCK_PATH, "run", {"seeds": seeds, "target_emails": args.target_emails}):
                run_pipeline(seeds, args.target_emails)
        elif args.command == "daily-run":
            deadline = time.monotonic() + DAILY_RUN_LOCK_RETRY_SECONDS
            while True:
                try:
                    with pipeline_lock(
                        LOCK_PATH,
                        "daily-run",
                        {
                            "target_emails": args.target_emails,
                            "seed_batch_size": args.seed_batch_size,
                            "max_cycles": args.max_cycles,
                            "hard_stop_hour_local": args.hard_stop_hour_local,
                        },
                    ):
                        run_daily(args.target_emails, args.seed_batch_size, args.max_cycles, args.hard_stop_hour_local)
                        break
                except PipelineBusyError as exc:
                    lock_command = str((exc.metadata or {}).get("command") or "")
                    if lock_command not in {"", "check-doc"}:
                        raise
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    sleep_for = min(DAILY_RUN_LOCK_RETRY_INTERVAL_SECONDS, max(remaining, 0))
                    blocker = lock_command or "another short-lived job"
                    print(f"Daily run waiting {sleep_for:.0f}s for {blocker} to release the pipeline lock...")
                    time.sleep(sleep_for)
        elif args.command == "check-doc":
            try:
                with pipeline_lock(LOCK_PATH, "check-doc", {"limit": args.limit}):
                    ensure_dirs()
                    cfg = api_config()
                    apify = ApifyHTTPClient(cfg["apify_token"])
                    supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
                    db = FinderDB(DB_PATH)
                    db.init()
                    counts = Counter()
                    started_at = utc_now_iso()
                    sync_worker_job_status(
                        supabase,
                        CRON_JOB_DOC_ID,
                        name="Finder V1 DOC Harvest",
                        schedule=DOC_HARVEST_SCHEDULE,
                        status="running",
                        started_at=started_at,
                        next_run_at=next_interval_iso(30),
                        increment_run_count=True,
                    )
                    if supabase.enabled():
                        counts["existing_today"] = supabase.count_today_net_new_emails(today_business_date())
                        print(f"Existing centralized emails for today: {counts['existing_today']}")
                    harvested = 0
                    doc_status = "success"
                    try:
                        duplicate_guard = build_duplicate_guard(cfg["smartlead_api_key"])
                        harvested = check_doc_jobs(db, counts, apify, supabase, duplicate_guard, limit=args.limit)
                    except Exception:
                        doc_status = "error"
                        raise
                    finally:
                        sync_worker_job_status(
                            supabase,
                            CRON_JOB_DOC_ID,
                            name="Finder V1 DOC Harvest",
                            schedule=DOC_HARVEST_SCHEDULE,
                            status=doc_status,
                            started_at=started_at,
                            next_run_at=next_interval_iso(30),
                        )
                        log_worker_event(
                            supabase,
                            "doc_harvest_completed" if doc_status == "success" else "doc_harvest_failed",
                            "ok" if doc_status == "success" else "error",
                            {
                                "day": today_business_date(),
                                "harvested": harvested,
                                "pending_doc_jobs": db.count_pending_doc_jobs(),
                                "counts": dict(counts),
                            },
                        )
                    print(json.dumps({"harvested": harvested, **dict(counts)}, indent=2))
            except PipelineBusyError as exc:
                print(json.dumps({
                    "status": "skipped",
                    "reason": "pipeline_busy",
                    "lock": exc.metadata,
                }, indent=2))
                return
        elif args.command == "refresh-results":
            with pipeline_lock(LOCK_PATH, "refresh-results"):
                ensure_dirs()
                cfg = api_config()
                supabase = SupabaseClient(cfg["supabase_url"], cfg["supabase_service_role_key"])
                db = FinderDB(DB_PATH)
                db.init()
                counts = refresh_saved_results(db, supabase if supabase.enabled() else None)
                latest_run = db.conn.execute("select id from runs order by id desc limit 1").fetchone()
                if latest_run:
                    paths = write_csvs(db, int(latest_run["id"]))
                    print(json.dumps({"run_id": int(latest_run["id"]), **dict(counts), **paths}, indent=2))
                else:
                    print(json.dumps(dict(counts), indent=2))
        elif args.command == "import-dashboard-db":
            with pipeline_lock(LOCK_PATH, "import-dashboard-db"):
                ensure_dirs()
                print(json.dumps(import_dashboard_database(), indent=2))
        elif args.command == "transcribe-reels":
            ensure_dirs()
            handle = args.handle.strip().lower().replace("@", "")
            print(json.dumps(transcribe_reels_for_handle(handle, args.limit), indent=2))
    except PipelineBusyError as exc:
        print(json.dumps({
            "error": "pipeline_busy",
            "message": str(exc),
            "lock": exc.metadata,
        }, indent=2))
        raise SystemExit(2)
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
