from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FinderDB:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

    def init(self) -> None:
        self.conn.executescript(
            """
            create table if not exists creators (
              handle text primary key,
              source_seed text,
              discovered_at text,
              profile_json text,
              followers integer,
              stage text default 'discovered',
              enriched_at text,
              qualified integer,
              reject_reason text,
              qualification_confidence real,
              qualification_why text,
              qualification_version text,
              best_email text,
              best_email_type text,
              best_email_source text,
              best_email_notes text,
              contact_searched_at text,
              youtube_fallback_at text,
              youtube_fallback_status text,
              updated_at text
            );

            create table if not exists runs (
              id integer primary key autoincrement,
              started_at text not null,
              finished_at text,
              seeds_json text not null,
              target_emails integer not null,
              summary_json text,
              output_csv text,
              audit_csv text
            );

            create table if not exists email_candidates (
              id integer primary key autoincrement,
              creator_handle text not null,
              email text not null,
              email_type text not null,
              source_method text not null,
              source_url text,
              source_path text,
              keep integer not null,
              notes text,
              created_at text not null
            );

            create table if not exists stage_results (
              creator_handle text not null,
              stage text not null,
              version text not null,
              status text not null,
              details_json text,
              updated_at text not null,
              primary key (creator_handle, stage)
            );

            create table if not exists doc_jobs (
              id integer primary key autoincrement,
              creator_handle text not null,
              youtube_channel text not null,
              apify_run_id text not null unique,
              dataset_id text,
              apify_status text,
              doc_status text,
              results_collected integer not null default 0,
              resurrect_count integer not null default 0,
              submitted_at text not null,
              last_checked_at text,
              completed_at text,
              last_error text,
              status_notes text
            );

            create table if not exists daily_progress (
              day text primary key,
              target_emails integer not null,
              started_at text not null,
              updated_at text not null,
              status text not null default 'running',
              current_count integer not null default 0,
              cycles_completed integer not null default 0,
              zero_progress_cycles integer not null default 0,
              last_progress_at text,
              summary_json text,
              state_json text
            );

            create table if not exists seed_performance (
              seed_handle text primary key,
              total_runs integer not null default 0,
              total_discovered integer not null default 0,
              total_enriched integer not null default 0,
              total_qualified integer not null default 0,
              total_kept integer not null default 0,
              total_doc_submitted integer not null default 0,
              total_doc_harvested integer not null default 0,
              last_used_at text,
              updated_at text not null
            );

            create table if not exists seed_usage (
              day text not null,
              seed_handle text not null,
              cycle_index integer not null default 0,
              discovered integer not null default 0,
              enriched integer not null default 0,
              qualified integer not null default 0,
              kept integer not null default 0,
              doc_submitted integer not null default 0,
              doc_harvested integer not null default 0,
              last_outcome text,
              used_at text not null,
              primary key (day, seed_handle)
            );
            """
        )
        self.conn.commit()

    def create_run(self, seeds: List[str], target_emails: int) -> int:
        cur = self.conn.execute(
            "insert into runs(started_at, seeds_json, target_emails) values (?, ?, ?)",
            (utc_now(), json.dumps(seeds), target_emails),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, summary: Dict[str, Any], output_csv: str, audit_csv: str) -> None:
        self.conn.execute(
            "update runs set finished_at = ?, summary_json = ?, output_csv = ?, audit_csv = ? where id = ?",
            (utc_now(), json.dumps(summary), output_csv, audit_csv, run_id),
        )
        self.conn.commit()

    def checkpoint_run(self, run_id: int, summary: Dict[str, Any], output_csv: str, audit_csv: str) -> None:
        self.conn.execute(
            "update runs set summary_json = ?, output_csv = ?, audit_csv = ? where id = ?",
            (json.dumps(summary), output_csv, audit_csv, run_id),
        )
        self.conn.commit()

    def upsert_creator(self, handle: str, seed: str) -> None:
        now = utc_now()
        self.conn.execute(
            """
            insert into creators(handle, source_seed, discovered_at, updated_at)
            values (?, ?, ?, ?)
            on conflict(handle) do update set
              source_seed = coalesce(creators.source_seed, excluded.source_seed),
              updated_at = excluded.updated_at
            """,
            (handle, seed, now, now),
        )
        self.conn.commit()

    def creator_exists(self, handle: str) -> bool:
        row = self.conn.execute("select 1 from creators where handle = ?", (handle,)).fetchone()
        return row is not None

    def set_profile(self, handle: str, profile: Dict[str, Any], followers: Optional[int], stage: str = "enriched") -> None:
        now = utc_now()
        self.conn.execute(
            """
            update creators
            set profile_json = ?, followers = ?, stage = ?, enriched_at = ?, updated_at = ?
            where handle = ?
            """,
            (json.dumps(profile), followers, stage, now, now, handle),
        )
        self.conn.commit()

    def set_qualification(
        self,
        handle: str,
        qualified: bool,
        reject_reason: str,
        confidence: float,
        why: str,
        version: str,
    ) -> None:
        now = utc_now()
        self.conn.execute(
            """
            update creators
            set qualified = ?, reject_reason = ?, qualification_confidence = ?, qualification_why = ?,
                qualification_version = ?, stage = ?, updated_at = ?
            where handle = ?
            """,
            (1 if qualified else 0, reject_reason or None, confidence, why, version,
             "qualified" if qualified else "rejected", now, handle),
        )
        self.conn.execute(
            """
            insert into stage_results(creator_handle, stage, version, status, details_json, updated_at)
            values (?, 'qualification', ?, ?, ?, ?)
            on conflict(creator_handle, stage) do update set
              version = excluded.version,
              status = excluded.status,
              details_json = excluded.details_json,
              updated_at = excluded.updated_at
            """,
            (handle, version, "qualified" if qualified else "rejected",
             json.dumps({"qualified": qualified, "reject_reason": reject_reason, "why": why, "confidence": confidence}), now),
        )
        self.conn.commit()

    def record_contact_result(
        self,
        handle: str,
        email: Optional[str],
        email_type: Optional[str],
        source: Optional[str],
        notes: str,
    ) -> None:
        now = utc_now()
        stage = "contact_found" if email else "contact_none"
        self.conn.execute(
            """
            update creators
            set best_email = ?, best_email_type = ?, best_email_source = ?, best_email_notes = ?,
                contact_searched_at = ?, stage = ?, updated_at = ?
            where handle = ?
            """,
            (email, email_type, source, notes, now, stage, now, handle),
        )
        self.conn.commit()

    def record_youtube_fallback(self, handle: str, status: str) -> None:
        self.conn.execute(
            "update creators set youtube_fallback_at = ?, youtube_fallback_status = ?, updated_at = ? where handle = ?",
            (utc_now(), status, utc_now(), handle),
        )
        self.conn.commit()

    def add_email_candidate(
        self,
        handle: str,
        email: str,
        email_type: str,
        source_method: str,
        source_url: str,
        source_path: str,
        keep: bool,
        notes: str,
    ) -> None:
        self.conn.execute(
            """
            insert into email_candidates(
              creator_handle, email, email_type, source_method, source_url, source_path, keep, notes, created_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (handle, email, email_type, source_method, source_url, source_path, 1 if keep else 0, notes, utc_now()),
        )
        self.conn.commit()

    def iter_email_candidates(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """
            select id, creator_handle, email, email_type, source_method, source_url, source_path, keep, notes
            from email_candidates
            order by creator_handle asc, created_at asc, id asc
            """
        )

    def list_email_candidates_for_creator(self, handle: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            select id, creator_handle, email, email_type, source_method, source_url, source_path, keep, notes
            from email_candidates
            where creator_handle = ?
            order by created_at asc, id asc
            """,
            (handle,),
        ).fetchall()

    def update_email_candidate(
        self,
        candidate_id: int,
        email_type: str,
        keep: bool,
        notes: str,
    ) -> None:
        self.conn.execute(
            """
            update email_candidates
            set email_type = ?, keep = ?, notes = ?
            where id = ?
            """,
            (email_type, 1 if keep else 0, notes, candidate_id),
        )
        self.conn.commit()

    def has_email_candidate(self, handle: str, email: str, source_method: str) -> bool:
        row = self.conn.execute(
            """
            select 1
            from email_candidates
            where creator_handle = ? and lower(email) = lower(?) and source_method = ?
            limit 1
            """,
            (handle, email, source_method),
        ).fetchone()
        return row is not None

    def upsert_doc_job(
        self,
        handle: str,
        youtube_channel: str,
        run_id: str,
        dataset_id: str,
        apify_status: str,
        doc_status: str,
        notes: str = "",
    ) -> None:
        now = utc_now()
        self.conn.execute(
            """
            insert into doc_jobs(
              creator_handle, youtube_channel, apify_run_id, dataset_id, apify_status, doc_status,
              submitted_at, last_checked_at, status_notes
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(apify_run_id) do update set
              dataset_id = excluded.dataset_id,
              apify_status = excluded.apify_status,
              doc_status = excluded.doc_status,
              last_checked_at = excluded.last_checked_at,
              status_notes = excluded.status_notes
            """,
            (handle, youtube_channel, run_id, dataset_id, apify_status, doc_status, now, now, notes),
        )
        self.conn.commit()

    def get_open_doc_job(self, handle: str, youtube_channel: str):
        return self.conn.execute(
            """
            select *
            from doc_jobs
            where creator_handle = ?
              and youtube_channel = ?
              and completed_at is null
            order by submitted_at desc
            limit 1
            """,
            (handle, youtube_channel),
        ).fetchone()

    def get_doc_job(self, run_id: str):
        return self.conn.execute(
            "select * from doc_jobs where apify_run_id = ?",
            (run_id,),
        ).fetchone()

    def list_pending_doc_jobs(self):
        return self.conn.execute(
            """
            select *
            from doc_jobs
            where completed_at is null
            order by submitted_at asc
            """
        ).fetchall()

    def count_pending_doc_jobs(self) -> int:
        row = self.conn.execute(
            "select count(*) as count from doc_jobs where completed_at is null"
        ).fetchone()
        return int(row["count"] if row else 0)

    def update_doc_job_status(
        self,
        run_id: str,
        *,
        apify_status: Optional[str] = None,
        doc_status: Optional[str] = None,
        dataset_id: Optional[str] = None,
        results_collected: Optional[int] = None,
        notes: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        existing = self.get_doc_job(run_id)
        if not existing:
            return
        self.conn.execute(
            """
            update doc_jobs
            set apify_status = ?, doc_status = ?, dataset_id = ?, results_collected = ?,
                last_checked_at = ?, status_notes = ?, last_error = ?
            where apify_run_id = ?
            """,
            (
                apify_status if apify_status is not None else existing["apify_status"],
                doc_status if doc_status is not None else existing["doc_status"],
                dataset_id if dataset_id is not None else existing["dataset_id"],
                results_collected if results_collected is not None else existing["results_collected"],
                utc_now(),
                notes if notes is not None else existing["status_notes"],
                error if error is not None else existing["last_error"],
                run_id,
            ),
        )
        self.conn.commit()

    def mark_doc_job_completed(self, run_id: str, note: str = "") -> None:
        self.conn.execute(
            """
            update doc_jobs
            set completed_at = ?, last_checked_at = ?, status_notes = coalesce(?, status_notes)
            where apify_run_id = ?
            """,
            (utc_now(), utc_now(), note or None, run_id),
        )
        self.conn.commit()

    def increment_doc_job_resurrect_count(
        self,
        old_run_id: str,
        new_run_id: str,
        dataset_id: str,
        apify_status: str,
        note: str,
    ) -> None:
        existing = self.get_doc_job(old_run_id)
        if not existing:
            return
        self.conn.execute(
            """
            update doc_jobs
            set apify_run_id = ?, dataset_id = ?, apify_status = ?, resurrect_count = ?, last_checked_at = ?, status_notes = ?
            where apify_run_id = ?
            """,
            (
                new_run_id,
                dataset_id or existing["dataset_id"],
                apify_status,
                int(existing["resurrect_count"] or 0) + 1,
                utc_now(),
                note,
                old_run_id,
            ),
        )
        self.conn.commit()

    def get_profile(self, handle: str) -> Dict[str, Any]:
        row = self.conn.execute("select profile_json from creators where handle = ?", (handle,)).fetchone()
        if not row or not row["profile_json"]:
            return {}
        return json.loads(row["profile_json"])

    def get_creator(self, handle: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("select * from creators where handle = ?", (handle,)).fetchone()

    def list_creator_handles(self) -> List[str]:
        rows = self.conn.execute("select handle from creators order by handle asc").fetchall()
        return [row["handle"] for row in rows]

    def set_best_contact_snapshot(
        self,
        handle: str,
        email: Optional[str],
        email_type: Optional[str],
        source: Optional[str],
        notes: str,
    ) -> None:
        creator = self.get_creator(handle)
        if not creator:
            return
        stage = creator["stage"]
        if email:
            stage = "contact_found"
        elif creator["qualified"] == 1:
            stage = "contact_none"
        self.conn.execute(
            """
            update creators
            set best_email = ?, best_email_type = ?, best_email_source = ?, best_email_notes = ?,
                stage = ?, updated_at = ?
            where handle = ?
            """,
            (email, email_type, source, notes, stage, utc_now(), handle),
        )
        self.conn.commit()

    def list_by_stage(self, stage: str) -> List[sqlite3.Row]:
        return self.conn.execute("select * from creators where stage = ? order by updated_at desc", (stage,)).fetchall()

    def list_qualified_without_email(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            select * from creators
            where qualified = 1 and best_email is null
            order by coalesce(followers, 0) desc
            """
        ).fetchall()

    def count_kept_emails_for_run(self) -> int:
        row = self.conn.execute(
            "select count(*) as count from creators where best_email is not null and best_email_type in ('personal', 'management')"
        ).fetchone()
        return int(row["count"] if row else 0)

    def count_kept_emails_since(self, started_at: str) -> int:
        row = self.conn.execute(
            """
            select count(*) as count
            from creators
            where updated_at >= ?
              and best_email is not null
              and best_email_type in ('personal', 'management')
            """,
            (started_at,),
        ).fetchone()
        return int(row["count"] if row else 0)

    def get_daily_progress(self, day: str):
        return self.conn.execute(
            "select * from daily_progress where day = ?",
            (day,),
        ).fetchone()

    def upsert_daily_progress(
        self,
        day: str,
        *,
        target_emails: int,
        status: str,
        current_count: int,
        cycles_completed: int,
        zero_progress_cycles: int,
        summary: Dict[str, Any],
        state: Dict[str, Any],
        started_at: str | None = None,
        last_progress_at: str | None = None,
    ) -> None:
        existing = self.get_daily_progress(day)
        now = utc_now()
        effective_started = started_at or (existing["started_at"] if existing else now)
        self.conn.execute(
            """
            insert into daily_progress(
              day, target_emails, started_at, updated_at, status, current_count,
              cycles_completed, zero_progress_cycles, last_progress_at, summary_json, state_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(day) do update set
              target_emails = excluded.target_emails,
              updated_at = excluded.updated_at,
              status = excluded.status,
              current_count = excluded.current_count,
              cycles_completed = excluded.cycles_completed,
              zero_progress_cycles = excluded.zero_progress_cycles,
              last_progress_at = excluded.last_progress_at,
              summary_json = excluded.summary_json,
              state_json = excluded.state_json
            """,
            (
                day,
                target_emails,
                effective_started,
                now,
                status,
                current_count,
                cycles_completed,
                zero_progress_cycles,
                last_progress_at,
                json.dumps(summary),
                json.dumps(state),
            ),
        )
        self.conn.commit()

    def record_seed_usage(
        self,
        day: str,
        seed_handle: str,
        *,
        cycle_index: int,
        discovered: int,
        enriched: int,
        qualified: int,
        kept: int,
        doc_submitted: int,
        doc_harvested: int,
        outcome: str,
    ) -> None:
        now = utc_now()
        self.conn.execute(
            """
            insert into seed_usage(
              day, seed_handle, cycle_index, discovered, enriched, qualified, kept,
              doc_submitted, doc_harvested, last_outcome, used_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(day, seed_handle) do update set
              cycle_index = excluded.cycle_index,
              discovered = excluded.discovered,
              enriched = excluded.enriched,
              qualified = excluded.qualified,
              kept = excluded.kept,
              doc_submitted = excluded.doc_submitted,
              doc_harvested = excluded.doc_harvested,
              last_outcome = excluded.last_outcome,
              used_at = excluded.used_at
            """,
            (
                day,
                seed_handle,
                cycle_index,
                discovered,
                enriched,
                qualified,
                kept,
                doc_submitted,
                doc_harvested,
                outcome,
                now,
            ),
        )
        self.conn.commit()

    def list_used_seeds_for_day(self, day: str) -> List[str]:
        rows = self.conn.execute(
            "select seed_handle from seed_usage where day = ? order by used_at asc",
            (day,),
        ).fetchall()
        return [row["seed_handle"] for row in rows]

    def list_seed_usage_for_day(self, day: str):
        return self.conn.execute(
            """
            select day, seed_handle, cycle_index, discovered, enriched, qualified, kept,
                   doc_submitted, doc_harvested, last_outcome, used_at
            from seed_usage
            where day = ?
            order by used_at asc, seed_handle asc
            """,
            (day,),
        ).fetchall()

    def update_seed_performance(
        self,
        seed_handle: str,
        *,
        discovered: int,
        enriched: int,
        qualified: int,
        kept: int,
        doc_submitted: int,
        doc_harvested: int,
    ) -> None:
        existing = self.conn.execute(
            "select * from seed_performance where seed_handle = ?",
            (seed_handle,),
        ).fetchone()
        now = utc_now()
        if existing:
            self.conn.execute(
                """
                update seed_performance
                set total_runs = total_runs + 1,
                    total_discovered = total_discovered + ?,
                    total_enriched = total_enriched + ?,
                    total_qualified = total_qualified + ?,
                    total_kept = total_kept + ?,
                    total_doc_submitted = total_doc_submitted + ?,
                    total_doc_harvested = total_doc_harvested + ?,
                    last_used_at = ?,
                    updated_at = ?
                where seed_handle = ?
                """,
                (
                    discovered,
                    enriched,
                    qualified,
                    kept,
                    doc_submitted,
                    doc_harvested,
                    now,
                    now,
                    seed_handle,
                ),
            )
        else:
            self.conn.execute(
                """
                insert into seed_performance(
                  seed_handle, total_runs, total_discovered, total_enriched, total_qualified,
                  total_kept, total_doc_submitted, total_doc_harvested, last_used_at, updated_at
                ) values (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    seed_handle,
                    discovered,
                    enriched,
                    qualified,
                    kept,
                    doc_submitted,
                    doc_harvested,
                    now,
                    now,
                ),
            )
        self.conn.commit()

    def list_seed_performance(self):
        return self.conn.execute(
            """
            select *
            from seed_performance
            order by total_kept desc, total_qualified desc, total_discovered desc, seed_handle asc
            """
        ).fetchall()

    def iter_export_rows(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """
            select handle, source_seed, followers, qualified, reject_reason, best_email, best_email_type,
                   best_email_source, best_email_notes, profile_json, stage
            from creators
            order by coalesce(followers, 0) desc, handle asc
            """
        )
