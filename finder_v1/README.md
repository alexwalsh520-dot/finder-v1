# Finder V1

`finder_v1` is a clean, script-first rebuild of the project.

It does one job:
- discover related Instagram creators from human-qualified Supabase seeds
- enrich profiles
- reject creators under `100K` followers or with obvious non-English bios
- search deeply for emails
- submit YouTube channels to DataOverCoffee and harvest them later
- export a clean CSV and an audit CSV

## Why this version is different
- no dashboard
- no agent framework
- no fake product shell
- local SQLite state plus optional Supabase sync
- stage-by-stage caching
- pending DOC jobs are tracked instead of disappearing

## Environment
Create `finder_v1/.env` with:

```env
APIFY_API_TOKEN=
YOUTUBE_API_KEY=
SMARTLEAD_API_KEY=
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
FINDER_TIMEZONE=Asia/Makassar
FINDER_OUTPUT_BUCKET=finder-outputs
```

`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are required for the live daily flow because qualified Supabase leads are the seed source.

## Run
From the repo root:

```bash
python3 -m finder_v1.main run --seeds cbum,davidlaid --target-emails 25
```

## Operating Loop
Use these commands in production:

1. Harvest pending DOC jobs:

```bash
python3 -m finder_v1.main check-doc --limit 50
```

2. Rebuild the clean export after classification changes or big backlog harvests:

```bash
python3 -m finder_v1.main refresh-results
```

3. Run the full daily operator until the target is hit or the day stops:

```bash
python3 -m finder_v1.main daily-run --target-emails 150 --hard-stop-hour-local 22
```

4. Check machine-readable status for today:

```bash
python3 -m finder_v1.main status
```

5. Run a cloud-readiness check before deploying:

```bash
python3 -m finder_v1.main doctor
```

6. If a local run was interrupted and left stale "running" state behind:

```bash
python3 -m finder_v1.main repair-state
```

7. Reconcile Smartlead sent state back into Supabase:

```bash
python3 -m finder_v1.main reconcile-smartlead --limit 200
```

Outputs:
- `finder_v1/output/run_<id>_results.csv`
- `finder_v1/output/run_<id>_audit.csv`
- `finder_v1/output/run_<id>_review.csv`
- `finder_v1/output/daily_<YYYY-MM-DD>_summary.json`
- `finder_v1/output/daily_<YYYY-MM-DD>_seed_report.csv`
- `finder_v1/state/finder.sqlite3`
- `finder_v1/state/pipeline.lock`

If `FINDER_OUTPUT_BUCKET` is set, the worker also uploads:
- review CSV
- results CSV
- audit CSV
- daily summary JSON
- seed report CSV

The upload paths look like:
- `finder-v1/daily/<YYYY-MM-DD>/run_<id>_review.csv`
- `finder-v1/daily/<YYYY-MM-DD>/run_<id>_results.csv`
- `finder-v1/daily/<YYYY-MM-DD>/run_<id>_audit.csv`
- `finder-v1/daily/<YYYY-MM-DD>/daily_<YYYY-MM-DD>_summary.json`
- `finder-v1/daily/<YYYY-MM-DD>/daily_<YYYY-MM-DD>_seed_report.csv`

## Notes for Cloud Deployment
- `daily-run`, `check-doc`, `refresh-results`, and `import-dashboard-db` now use a shared lock file so two overlapping jobs do not mutate the pipeline at the same time.
- `status` and `doctor` are designed to give a future AI overseer simple, machine-readable signals instead of forcing it to parse messy logs.
- `daily-run` uses Bali time (`Asia/Makassar`) for daily counting and the local stop hour.
- `daily-run` uses only human-qualified Supabase leads as seeds and records each seed in `discovery_seed_expansions` after one related-account expansion.
- `doctor` now checks Supabase Storage access in addition to the lead schema.
- the worker writes plain status rows to `cron_jobs` and plain factual event rows to `agent_events` using the component name `finder_v1_worker`
- the worker also writes a remote heartbeat snapshot to the `app_settings` key `finder_v1_runtime_status` so live run truth survives temporary SSH problems
- seed discovery now suppresses handles that already have a live email or delivery state before profile enrichment, which reduces wasted enrichment work on obvious duplicates
- the daily seed report now includes duplicate waste and live-skip waste metrics for each seed
- DOC polling now records more precise failure counters in the daily summary, including forbidden payloads, empty payloads, duplicate-only candidate sets, and done-without-candidate outcomes
- If `doctor` says Smartlead tracking columns are missing, apply `supabase/migrations/003_smartlead_tracking.sql` from this repo in the Supabase SQL editor before cloud launch.
- If `doctor` says review app fields are missing, apply `supabase/migrations/004_finder_review_app.sql` from this repo in the Supabase SQL editor before launching the review app.
- If `doctor` says the seed expansion table is missing, apply `supabase/migrations/006_discovery_seed_expansions.sql` from this repo in the Supabase SQL editor before running the daily scraper.
