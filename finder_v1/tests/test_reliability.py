from __future__ import annotations

import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from finder_v1.db import FinderDB
from finder_v1.main import build_seed_rankings, select_seed_batch, write_daily_seed_report_csv, write_daily_summary_json
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

    def test_select_seed_batch_prefers_stronger_unused_seeds(self) -> None:
        self.db.update_seed_performance(
            "alpha",
            discovered=100,
            enriched=80,
            qualified=20,
            kept=8,
            doc_submitted=3,
            doc_harvested=2,
        )
        self.db.update_seed_performance(
            "beta",
            discovered=120,
            enriched=100,
            qualified=5,
            kept=1,
            doc_submitted=1,
            doc_harvested=0,
        )
        self.db.record_seed_usage(
            "2026-04-13",
            "beta",
            cycle_index=1,
            discovered=5,
            enriched=5,
            qualified=0,
            kept=0,
            doc_submitted=0,
            doc_harvested=0,
            outcome="no_progress",
        )

        batch = select_seed_batch(self.db, "2026-04-13", batch_size=2, zero_progress_cycles=0)
        self.assertEqual(batch[0], "alpha")
        self.assertNotIn("beta", batch)

    def test_seed_report_and_summary_are_written(self) -> None:
        self.db.update_seed_performance(
            "alpha",
            discovered=50,
            enriched=40,
            qualified=10,
            kept=4,
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
        self.assertIn("top_seed_candidates", payload)

    def test_pipeline_lock_blocks_second_holder(self) -> None:
        lock_path = self.temp_path / "pipeline.lock"
        with pipeline_lock(lock_path, "daily-run", {"target_emails": 100}):
            with self.assertRaises(PipelineBusyError):
                with pipeline_lock(lock_path, "check-doc"):
                    pass

    def test_build_seed_rankings_includes_fallbacks(self) -> None:
        rankings = build_seed_rankings(self.db, "2026-04-13")
        handles = [row["seed_handle"] for row in rankings]
        self.assertIn("cbum", handles)
        self.assertIn("simeonpanda", handles)


if __name__ == "__main__":
    unittest.main()
