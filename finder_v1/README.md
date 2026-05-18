# Finder V1

`finder_v1` is a clean, script-first rebuild of the project.

It does one job:
- discover related Instagram creators from seed handles
- enrich profiles
- reject creators under `100K` followers
- qualify the remaining creators
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
DATAOVERCOFFEE_API_KEY=
ANTHROPIC_API_KEY=
YOUTUBE_API_KEY=
SMARTLEAD_API_KEY=
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
FINDER_TIMEZONE=Asia/Makassar
FINDER_OUTPUT_BUCKET=finder-outputs
```

`ANTHROPIC_API_KEY` is optional. If it is missing, qualification stays deterministic.
`YOUTUBE_API_KEY`, `SUPABASE_URL`, and `SUPABASE_SERVICE_ROLE_KEY` are optional but recommended for the live operating flow.

Optional qualification gate:

```env
REQUIRE_GREEN_LIGHT_BRAND_IN_BIO=1
GREEN_LIGHT_BRANDS_FILE=/absolute/path/to/green-light-fitness-brands.txt
```

When `REQUIRE_GREEN_LIGHT_BRAND_IN_BIO=1`, a creator is rejected before email/YouTube enrichment unless their Instagram bio contains at least one approved brand `@username`. `GREEN_LIGHT_BRANDS_FILE` is optional; if omitted, the built-in fitness brand list is used.

## Run
From the repo root:

```bash
python3 -m finder_v1.main run --seeds cbum,davidlaid --target-emails 25
```

## Operating Loop
Use these two commands in production:

1. Submit and process new work:

```bash
python3 -m finder_v1.main run --seeds cbum,simeonpanda,mikeohearn --target-emails 25
```

2. Harvest pending DOC jobs later:

```bash
python3 -m finder_v1.main check-doc --limit 300
```

3. Rebuild the clean export after classification changes or big backlog harvests:

```bash
python3 -m finder_v1.main refresh-results
```

4. Run the full daily operator until the target is hit or the day stops:

```bash
python3 -m finder_v1.main daily-run --target-emails 150 --hard-stop-hour-local 22
```

5. Check machine-readable status for today:

```bash
python3 -m finder_v1.main status
```

6. Run a cloud-readiness check before deploying:

```bash
python3 -m finder_v1.main doctor
```

7. If a local run was interrupted and left stale "running" state behind:

```bash
python3 -m finder_v1.main repair-state
```

8. Reconcile Smartlead sent state back into Supabase:

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
- `doctor` now checks Supabase Storage access in addition to the lead schema.
- the worker writes plain status rows to `cron_jobs` and plain factual event rows to `agent_events` using the component name `finder_v1_worker`
- If `doctor` says Smartlead tracking columns are missing, apply `/Users/alexwalsh/Documents/New project/supabase/migrations/003_smartlead_tracking.sql` in the Supabase SQL editor before cloud launch.
- If `doctor` says review app fields are missing, apply `/Users/alexwalsh/Documents/New project/supabase/migrations/004_finder_review_app.sql` in the Supabase SQL editor before launching the review app.
