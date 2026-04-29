from __future__ import annotations

import json
import tempfile
import time
import unittest
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from finder_v1.db import FinderDB
from finder_v1.email_search import classify_email, deep_email_search
from finder_v1.qualification import deterministic_qualification
from finder_v1.main import (
    build_seed_history,
    classify_reviewable_email,
    compact_smartlead_reconcile_event_data,
    counts_as_net_new_email,
    filter_duplicate_email_candidates,
    filter_existing_live_handles_before_enrich,
    daily_hard_stop_reached,
    live_row_blocks_processing,
    run_daily,
    remember_session_email,
    scrub_invalid_reviewable_emails,
    select_seed_batch,
    sync_best_to_supabase,
    sync_runtime_heartbeat,
    validate_target_emails,
    write_daily_review_csv,
    write_daily_seed_report_csv,
    write_daily_summary_json,
    write_new_results_csv,
)
from finder_v1.runtime_lock import PipelineBusyError, pipeline_lock
from finder_v1.supabase_client import SupabaseClient


class ReliabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.tempdir.name)
        self.db_path = self.temp_path / "finder.sqlite3"
        self.db = FinderDB(self.db_path)
        self.db.init()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_select_seed_batch_uses_unexpanded_supabase_qualified_seeds(self) -> None:
        class FakeSupabase:
            def enabled(self) -> bool:
                return True

            def fetch_unexpanded_qualified_seed_handles(self, *, limit: int = 5000):
                return ["alpha", "bravo", "charlie"]

        batch = select_seed_batch(FakeSupabase(), batch_size=2)
        self.assertEqual(batch, ["alpha", "bravo"])

    def test_select_seed_batch_stops_without_supabase(self) -> None:
        class FakeSupabase:
            def enabled(self) -> bool:
                return False

        self.assertEqual(select_seed_batch(FakeSupabase(), batch_size=2), [])

    def test_daily_hard_stop_stays_reached_after_midnight(self) -> None:
        bali = ZoneInfo("Asia/Makassar")

        self.assertFalse(
            daily_hard_stop_reached(
                "2026-04-28",
                22,
                now=datetime(2026, 4, 28, 21, 59, tzinfo=bali),
            )
        )
        self.assertTrue(
            daily_hard_stop_reached(
                "2026-04-28",
                22,
                now=datetime(2026, 4, 28, 22, 0, tzinfo=bali),
            )
        )
        self.assertTrue(
            daily_hard_stop_reached(
                "2026-04-28",
                22,
                now=datetime(2026, 4, 29, 1, 0, tzinfo=bali),
            )
        )

    def test_supabase_sync_can_pin_batch_date_to_run_day(self) -> None:
        class FakeSupabase:
            def __init__(self) -> None:
                self.payloads = []

            def upsert_lead(self, payload):
                self.payloads.append(payload)

        fake_supabase = FakeSupabase()
        counts = Counter()

        sync_best_to_supabase(
            fake_supabase,
            counts,
            "creator",
            250000,
            {"full_name": "Creator Person", "biography": "Training daily"},
            {
                "email": "creator@example.com",
                "email_type": "personal",
                "source_method": "doc",
            },
            "seed",
            batch_date="2026-04-28",
        )

        self.assertEqual(fake_supabase.payloads[0]["batch_date"], "2026-04-28")
        self.assertEqual(counts["synced_to_supabase"], 1)

    def test_simple_qualification_uses_only_hard_gates_after_seed_relation(self) -> None:
        qualified = deterministic_qualification(
            {
                "username": "qualifiedseedrelated",
                "full_name": "Qualified Seed Related",
                "biography": "Building every day. Links below @brandhandle",
                "business_category": "",
                "recent_captions": [],
                "external_url": "",
            }
        )
        spanish = deterministic_qualification(
            {
                "username": "fitcreator",
                "full_name": "Fit Creator",
                "biography": "Entrenadora online. Contacto por correo.",
                "business_category": "Fitness trainer",
                "recent_captions": [],
                "external_url": "",
            }
        )
        store = deterministic_qualification(
            {
                "username": "creatorstore",
                "full_name": "Creator Official Store",
                "biography": "Official store. Shipping worldwide.",
                "business_category": "",
                "recent_captions": [],
                "external_url": "",
            }
        )

        self.assertTrue(qualified["qualified"])
        self.assertFalse(spanish["qualified"])
        self.assertEqual(spanish["reject_reason"], "not_english")
        self.assertFalse(store["qualified"])
        self.assertEqual(store["reject_reason"], "not_person")

    def test_non_english_gate_ignores_mentions_and_urls(self) -> None:
        result = deterministic_qualification(
            {
                "username": "englishcreator",
                "full_name": "English Creator",
                "biography": "Coaching and daily training. @marca_español https://例子.test",
                "business_category": "",
                "recent_captions": [],
                "external_url": "",
            }
        )
        self.assertTrue(result["qualified"])

    def test_finder_db_uses_wal_mode_for_worker_state(self) -> None:
        journal_mode = self.db.conn.execute("pragma journal_mode").fetchone()[0]
        synchronous = int(self.db.conn.execute("pragma synchronous").fetchone()[0])
        busy_timeout = int(self.db.conn.execute("pragma busy_timeout").fetchone()[0])

        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertEqual(synchronous, 1)
        self.assertEqual(busy_timeout, 30000)

    def test_pending_doc_jobs_prioritize_least_recently_checked(self) -> None:
        self.db.upsert_doc_job("alpha", "https://youtube.com/a", "run-alpha", "dataset-alpha", "RUNNING", "pending")
        time.sleep(0.001)
        self.db.upsert_doc_job("beta", "https://youtube.com/b", "run-beta", "dataset-beta", "RUNNING", "pending")
        time.sleep(0.001)
        self.db.update_doc_job_status("run-alpha", notes="Checked more recently")

        jobs = self.db.list_pending_doc_jobs()
        self.assertEqual([job["apify_run_id"] for job in jobs[:2]], ["run-beta", "run-alpha"])

    def test_deep_email_search_reuses_fetched_html_for_start_and_linked_pages(self) -> None:
        profile = {
            "biography": "",
            "external_url": "https://linktr.ee/testcreator",
        }
        html_map = {
            "https://linktr.ee/testcreator": '<a href="https://creator.example">site</a>',
            "https://creator.example": '<a href="mailto:coach@creator.example">email</a><a href="/contact">contact</a>',
            "https://creator.example/contact": '<a href="mailto:bookings@agency.com">email</a>',
        }
        calls = []

        def fake_fetch(url: str):
            calls.append(url)
            return html_map.get(url)

        with mock.patch("finder_v1.email_search.fetch_text", side_effect=fake_fetch):
            candidates = deep_email_search(profile)

        self.assertEqual(calls.count("https://linktr.ee/testcreator"), 1)
        self.assertEqual(calls.count("https://creator.example"), 1)
        self.assertIn("coach@creator.example", {row["email"] for row in candidates})
        self.assertIn("bookings@agency.com", {row["email"] for row in candidates})

    def test_seed_report_and_summary_are_written(self) -> None:
        self.db.update_seed_performance(
            "alpha",
            discovered=50,
            enriched=40,
            qualified=10,
            kept=4,
            skipped_existing_live=2,
            duplicate_emails=3,
            doc_submitted=2,
            doc_harvested=1,
        )
        self.db.record_seed_usage(
            "2026-04-13",
            "alpha",
            cycle_index=1,
            discovered=10,
            enriched=8,
            qualified=2,
            kept=1,
            skipped_existing_live=1,
            duplicate_emails=1,
            doc_submitted=1,
            doc_harvested=1,
            outcome="progress",
        )

        import finder_v1.main as main_mod

        original_output_dir = main_mod.OUTPUT_DIR
        try:
            main_mod.OUTPUT_DIR = self.temp_path
            seed_report = write_daily_seed_report_csv(self.db, "2026-04-13")
            summary_path = write_daily_summary_json(
                self.db,
                run_id=7,
                day="2026-04-13",
                started_at="2026-04-13T00:00:00+00:00",
                target_emails=100,
                counts=Counter({"current_daily_count": 12, "pace_status": "behind_pace"}),
                latest_seed_batch=["alpha"],
                paths={"final": "final.csv", "audit": "audit.csv", "review": "review.csv"},
                supabase=SupabaseClient("", ""),
            )
        finally:
            main_mod.OUTPUT_DIR = original_output_dir

        self.assertTrue(Path(seed_report).exists())
        self.assertTrue(Path(summary_path).exists())
        payload = json.loads(Path(summary_path).read_text())
        self.assertEqual(payload["run_id"], 7)
        self.assertEqual(payload["latest_seed_batch"], ["alpha"])
        self.assertIn("seed_history", payload)
        self.assertEqual(payload["results_csv_scope"], "inventory_snapshot")
        self.assertEqual(payload["inventory_total_emails"], 0)
        self.assertEqual(payload["new_local_emails_found_since_started_at"], 0)

    def test_pipeline_lock_blocks_second_holder(self) -> None:
        lock_path = self.temp_path / "pipeline.lock"
        with pipeline_lock(lock_path, "daily-run", {"target_emails": 100}):
            with self.assertRaises(PipelineBusyError):
                with pipeline_lock(lock_path, "check-doc"):
                    pass

    def test_build_seed_history_reports_only_actual_seed_history(self) -> None:
        self.db.update_seed_performance(
            "alpha",
            discovered=10,
            enriched=8,
            qualified=6,
            kept=2,
            skipped_existing_live=1,
            duplicate_emails=0,
            doc_submitted=1,
            doc_harvested=0,
        )
        history = build_seed_history(self.db, "2026-04-13")
        handles = [row["seed_handle"] for row in history]
        self.assertEqual(handles, ["alpha"])

    def test_best_email_found_at_is_stable_for_same_email(self) -> None:
        self.db.upsert_creator("alpha", "seed")
        self.db.record_contact_result("alpha", "alpha@example.com", "personal", "bio", "First capture")
        first = self.db.get_creator("alpha")
        first_found_at = first["best_email_found_at"]
        self.assertTrue(first_found_at)

        time.sleep(0.001)
        self.db.set_best_contact_snapshot("alpha", "alpha@example.com", "personal", "bio", "Refresh capture")
        second = self.db.get_creator("alpha")
        self.assertEqual(second["best_email_found_at"], first_found_at)

        time.sleep(0.001)
        self.db.set_best_contact_snapshot("alpha", "alpha2@example.com", "personal", "site", "New capture")
        third = self.db.get_creator("alpha")
        self.assertNotEqual(third["best_email_found_at"], first_found_at)

    def test_review_and_new_results_follow_email_found_time_not_discovered_time(self) -> None:
        self.db.upsert_creator("alpha", "seed")
        self.db.conn.execute(
            "update creators set discovered_at = ? where handle = ?",
            ("2026-04-10T00:00:00+00:00", "alpha"),
        )
        self.db.set_profile("alpha", {"username": "alpha", "external_url": "https://example.com"}, 123456)
        self.db.record_contact_result("alpha", "alpha@example.com", "personal", "bio", "First capture")
        creator = self.db.get_creator("alpha")
        found_at = datetime.fromisoformat(creator["best_email_found_at"].replace("Z", "+00:00"))
        started_at = (found_at - timedelta(seconds=1)).isoformat()

        import finder_v1.main as main_mod

        original_output_dir = main_mod.OUTPUT_DIR
        try:
            main_mod.OUTPUT_DIR = self.temp_path
            review_path = write_daily_review_csv(self.db, run_id=9, started_at=started_at)
            new_results_path = write_new_results_csv(self.db, run_id=9, started_at=started_at)
        finally:
            main_mod.OUTPUT_DIR = original_output_dir

        review_rows = Path(review_path).read_text().strip().splitlines()
        new_result_rows = Path(new_results_path).read_text().strip().splitlines()
        self.assertEqual(len(review_rows), 2)
        self.assertEqual(len(new_result_rows), 2)
        self.assertIn("alpha@example.com", new_result_rows[1])

    def test_validate_target_emails_rejects_zero(self) -> None:
        with self.assertRaises(RuntimeError):
            validate_target_emails(0, command_name="daily-run")

    def test_counts_as_net_new_email_excludes_auto_sent_duplicates(self) -> None:
        self.assertTrue(counts_as_net_new_email({"email": "a@example.com", "sent_to_smartlead": False, "reviewed_at": None}))
        self.assertTrue(counts_as_net_new_email({"email": "a@example.com", "sent_to_smartlead": True, "reviewed_at": "2026-04-14T00:00:00Z"}))
        self.assertFalse(counts_as_net_new_email({"email": "a@example.com", "sent_to_smartlead": True, "reviewed_at": None}))
        self.assertFalse(counts_as_net_new_email({"email": "", "sent_to_smartlead": False, "reviewed_at": None}))
        self.assertFalse(counts_as_net_new_email({"email": "a@example.com", "status": "youtube_only", "sent_to_smartlead": False, "reviewed_at": None}))

    def test_filter_duplicate_email_candidates_skips_smartlead_and_live_duplicates(self) -> None:
        class FakeSupabase:
            def enabled(self) -> bool:
                return True

            def fetch_existing_leads_by_email(self, email: str):
                if email == "live@example.com":
                    return [{"instagram_handle": "someone_else", "email": email}]
                return []

        counts = Counter()
        candidates = [
            {"email": "smartlead@example.com", "email_type": "personal", "source_method": "bio", "notes": ""},
            {"email": "live@example.com", "email_type": "personal", "source_method": "bio", "notes": ""},
            {"email": "fresh@example.com", "email_type": "personal", "source_method": "bio", "notes": ""},
        ]
        usable = filter_duplicate_email_candidates(
            FakeSupabase(),
            {"smartlead_emails": {"smartlead@example.com"}, "supabase_email_cache": {}},
            counts,
            "creator_handle",
            candidates,
        )

        self.assertEqual([item["email"] for item in usable], ["fresh@example.com"])
        self.assertEqual(counts["duplicate_existing_smartlead"], 1)
        self.assertEqual(counts["duplicate_existing_live_email"], 1)

    def test_smartlead_event_payload_is_compact(self) -> None:
        result = {
            "historical": False,
            "rate_limited": False,
            "summary": {"checked": 10, "confirmed_sent": 6},
            "matched": [
                {"id": index, "email": f"matched{index}@example.com", "instagram_handle": f"matched{index}"}
                for index in range(10)
            ],
            "unmatched": [
                {"id": index, "email": f"unmatched{index}@example.com", "instagram_handle": f"unmatched{index}"}
                for index in range(10)
            ],
        }

        payload = compact_smartlead_reconcile_event_data(result, limit=5000, historical=False)

        self.assertEqual(payload["matched_count"], 10)
        self.assertEqual(payload["unmatched_count"], 10)
        self.assertEqual(len(payload["matched_sample"]), 5)
        self.assertEqual(len(payload["unmatched_sample"]), 5)
        self.assertNotIn("matched", payload)
        self.assertNotIn("unmatched", payload)

    def test_filter_duplicate_email_candidates_skips_duplicates_seen_in_same_run(self) -> None:
        class FakeSupabase:
            def enabled(self) -> bool:
                return False

        counts = Counter()
        duplicate_guard = {
            "smartlead_emails": set(),
            "supabase_email_cache": {},
            "session_emails": {},
        }
        remember_session_email(duplicate_guard, "alpha", "shared@example.com")
        usable = filter_duplicate_email_candidates(
            FakeSupabase(),
            duplicate_guard,
            counts,
            "beta",
            [{"email": "shared@example.com", "email_type": "personal", "source_method": "bio", "notes": ""}],
        )

        self.assertEqual(usable, [])
        self.assertEqual(counts["duplicate_existing_live_email"], 1)

    def test_live_row_blocks_processing_requires_existing_email_or_delivery_state(self) -> None:
        self.assertTrue(live_row_blocks_processing({"email": "x@example.com"}))
        self.assertTrue(live_row_blocks_processing({"sent_to_smartlead": True}))
        self.assertTrue(live_row_blocks_processing({"smartlead_campaign_id": "abc"}))
        self.assertTrue(live_row_blocks_processing({"smartlead_sent_at": "2026-04-20T00:00:00Z"}))
        self.assertFalse(live_row_blocks_processing({"email": "", "sent_to_smartlead": False}))
        self.assertFalse(live_row_blocks_processing(None))

    def test_filter_existing_live_handles_before_enrich_skips_known_live_handles(self) -> None:
        class FakeSupabase:
            def enabled(self) -> bool:
                return True

            def fetch_existing_handles(self, handles):
                rows = {}
                if "live_creator" in handles:
                    rows["live_creator"] = {"instagram_handle": "live_creator", "email": "live@example.com"}
                if "sent_creator" in handles:
                    rows["sent_creator"] = {"instagram_handle": "sent_creator", "sent_to_smartlead": True}
                return rows

        filtered, skipped = filter_existing_live_handles_before_enrich(
            FakeSupabase(),
            ["fresh_creator", "live_creator", "sent_creator", "fresh_creator"],
        )

        self.assertEqual(filtered, ["fresh_creator", "fresh_creator"])
        self.assertEqual(skipped, 2)

    def test_sync_runtime_heartbeat_writes_off_box_status(self) -> None:
        class FakeSupabase:
            def __init__(self) -> None:
                self.saved = []

            def enabled(self) -> bool:
                return True

            def upsert_app_setting(self, key: str, value: dict) -> None:
                self.saved.append((key, value))

        fake = FakeSupabase()
        counts = Counter({"current_daily_count": 12, "pace_status": "behind_pace", "cycles_completed": 2, "zero_progress_cycles": 1})
        self.db.upsert_doc_job("alpha", "https://youtube.com/a", "run-alpha", "dataset-alpha", "RUNNING", "pending")

        sync_runtime_heartbeat(
            fake,
            self.db,
            day="2026-04-20",
            run_id=42,
            status="running",
            target_emails=150,
            counts=counts,
            started_at="2026-04-20T00:00:00+00:00",
            last_progress_at="2026-04-20T00:30:00+00:00",
            latest_seed_batch=["alpha", "beta"],
            output_paths={"summary": "daily_2026-04-20_summary.json"},
        )

        self.assertEqual(len(fake.saved), 1)
        key, payload = fake.saved[0]
        self.assertEqual(key, "finder_v1_runtime_status")
        self.assertEqual(payload["day"], "2026-04-20")
        self.assertEqual(payload["run_id"], 42)
        self.assertEqual(payload["pending_doc_jobs"], 1)
        self.assertEqual(payload["latest_seed_batch"], ["alpha", "beta"])

    def test_classify_email_rejects_placeholder_and_shared_business_inboxes(self) -> None:
        self.assertEqual(classify_email("name@mail.com", "bio")[0], "junk")
        self.assertEqual(classify_email("user@domain.com", "bio")[0], "junk")
        self.assertEqual(classify_email("you@email.com", "bio")[0], "junk")
        self.assertEqual(classify_email("creators@solinfitness.com", "https://solinfitness.com")[0], "generic_business")
        self.assertEqual(classify_email("preview.earlyaccess@oneractive.com", "https://oneractive.com")[0], "generic_business")
        self.assertEqual(classify_email("preview.diamond@oneractive.com", "https://oneractive.com")[0], "generic_business")
        self.assertEqual(classify_email("anfragen@projekt29.de", "https://www.williambonacsignature.com/legal")[0], "generic_business")
        self.assertEqual(classify_email("ecommerce@insta360.com", "https://insta360.com")[0], "generic_business")
        self.assertEqual(classify_email("help@skool.com", "https://skool.com")[0], "platform")
        self.assertEqual(classify_email("evie@themillaragency.com", "https://themillaragency.com")[0], "management")

    def test_classify_reviewable_email_uses_email_only_guardrails(self) -> None:
        self.assertEqual(classify_reviewable_email("preview.earlyaccess@oneractive.com")[0], "generic_business")
        self.assertEqual(classify_reviewable_email("name@mail.com")[0], "junk")
        self.assertEqual(classify_reviewable_email("evie@themillaragency.com")[0], "management")

    def test_scrub_invalid_reviewable_emails_downgrades_junk_unreviewed_rows(self) -> None:
        class FakeSupabase:
            def __init__(self) -> None:
                self.updated = []
                self.events = []

            def enabled(self) -> bool:
                return True

            def fetch_rows(self, table: str, select: str = "*", filters=None, limit=None):
                return [
                    {
                        "id": "bad-1",
                        "instagram_handle": "bad_handle",
                        "email": "name@mail.com",
                        "status": "email_ready",
                        "review_status": "unreviewed",
                        "batch_date": "2026-04-14",
                    },
                    {
                        "id": "good-1",
                        "instagram_handle": "good_handle",
                        "email": "evie@themillaragency.com",
                        "status": "mgmt_email",
                        "review_status": "unreviewed",
                        "batch_date": "2026-04-14",
                    },
                ]

            def bulk_update_leads(self, lead_ids, payload):
                self.updated.append((lead_ids, payload))

            def insert_agent_event(self, payload):
                self.events.append({"agent_event": payload})

        fake = FakeSupabase()
        result = scrub_invalid_reviewable_emails(fake)

        self.assertEqual(result["cleaned"], 1)
        self.assertEqual(result["by_kind"], {"junk": 1})
        self.assertEqual(len(fake.updated), 1)
        self.assertEqual(fake.updated[0][0], ["bad-1"])
        self.assertEqual(fake.updated[0][1]["status"], "no_email")
        self.assertEqual(fake.updated[0][1]["email"], None)
        self.assertTrue(any("agent_event" in event for event in fake.events if isinstance(event, dict)))

    def test_run_daily_stops_processing_seed_batch_once_target_is_reached(self) -> None:
        import finder_v1.main as main_mod

        class FakeSupabase:
            def __init__(self) -> None:
                self.current = 0
                self.job_payloads = []
                self.events = []
                self.settings = []
                self.seed_expansions = []

            def enabled(self) -> bool:
                return True

            def probe_leads_schema(self):
                return {"enabled": True, "reachable": True}

            def count_today_net_new_emails(self, day: str, *, source: str = "finder_v1") -> int:
                return self.current

            def fetch_rows(self, table: str, select: str = "*", filters=None, limit=None):
                if table == "cron_jobs" and self.job_payloads:
                    return [{"id": self.job_payloads[-1]["id"], "run_count": self.job_payloads[-1]["run_count"]}]
                return []

            def upsert_cron_job(self, payload):
                self.job_payloads.append(dict(payload))

            def insert_agent_event(self, payload):
                self.events.append(dict(payload))

            def upsert_app_setting(self, key: str, value: dict):
                self.settings.append((key, dict(value)))

            def record_seed_expansion(self, seed_handle: str, **kwargs):
                self.seed_expansions.append((seed_handle, kwargs))

        fake_supabase = FakeSupabase()
        processed_seeds: list[str] = []

        def fake_run_seed_cycle(db, counts, apify, youtube_key, supabase, duplicate_guard, run_id, seed, target_emails, started_at, batch_date=None):
            processed_seeds.append(seed)
            fake_supabase.current = target_emails
            counts["kept"] += target_emails
            return {"kept": target_emails, "discovered": 1, "enriched": 1, "qualified": 1, "profiles_checked": 1}

        with mock.patch.object(main_mod, "DB_PATH", self.db_path), \
             mock.patch.object(main_mod, "ensure_dirs"), \
             mock.patch.object(main_mod, "api_config", return_value={
                 "apify_token": "",
                 "youtube_key": "",
                 "supabase_url": "https://example.supabase.co",
                 "supabase_service_role_key": "service-role",
                 "smartlead_api_key": "",
                 "finder_output_bucket": "finder-outputs",
             }), \
             mock.patch.object(main_mod, "ApifyHTTPClient", return_value=mock.Mock(enabled=mock.Mock(return_value=False))), \
             mock.patch.object(main_mod, "SupabaseClient", return_value=fake_supabase), \
             mock.patch.object(main_mod, "build_duplicate_guard", return_value={"smartlead_emails": set(), "supabase_email_cache": {}, "session_emails": {}}), \
             mock.patch.object(main_mod, "scrub_invalid_reviewable_emails", return_value={"cleaned": 0, "by_kind": {}, "examples": []}), \
             mock.patch.object(main_mod, "today_business_date", return_value="2026-04-15"), \
             mock.patch.object(main_mod, "utc_now_iso", return_value="2026-04-15T00:00:00+00:00"), \
             mock.patch.object(main_mod, "next_business_time_iso", return_value="2026-04-16T02:00:00+08:00"), \
             mock.patch.object(main_mod, "business_now", return_value=datetime(2026, 4, 15, 10, 0, 0)), \
             mock.patch.object(main_mod, "select_seed_batch", return_value=["alpha", "beta"]), \
             mock.patch.object(main_mod, "run_seed_cycle", side_effect=fake_run_seed_cycle), \
             mock.patch.object(main_mod, "checkpoint_outputs_with_review", return_value={"final": "", "audit": "", "review": "", "summary": "", "seed_report": "", "new_results": ""}), \
             mock.patch.object(main_mod, "write_daily_seed_report_csv", return_value="seed-report.csv"), \
             mock.patch.object(main_mod, "write_daily_summary_json", return_value="daily-summary.json"), \
             mock.patch.object(main_mod, "upload_output_files", return_value={}):
            run_daily(target_emails=2, seed_batch_size=2, max_cycles=3, hard_stop_hour_local=22)

        self.assertEqual(processed_seeds, ["alpha"])
        daily_progress = self.db.get_daily_progress("2026-04-15")
        self.assertIsNotNone(daily_progress)
        self.assertEqual(daily_progress["status"], "complete")
        self.assertEqual(daily_progress["current_count"], 2)
        self.assertEqual(fake_supabase.job_payloads[-1]["last_status"], "complete")
        self.assertTrue(any(key == "finder_v1_runtime_status" for key, _ in fake_supabase.settings))
        self.assertEqual([seed for seed, _ in fake_supabase.seed_expansions], ["alpha"])


if __name__ == "__main__":
    unittest.main()
