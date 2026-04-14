# Finder V1 Cloud Deploy

This is the simple worker-only deploy for the DigitalOcean VM.

## Target shape
- one Ubuntu VM
- one Python virtual environment
- one local SQLite database on the VM
- one environment file at `/etc/finder-v1.env`
- four systemd jobs:
  - `finder-v1-repair-on-boot.service`
  - `finder-v1-daily-run.timer`
  - `finder-v1-doc-harvest.timer`
  - `finder-v1-smartlead-reconcile.timer`

## VM assumptions
- VM host: DigitalOcean
- server name: `finder-v1-worker`
- working directory: `/opt/finder-v1`
- app user: `finder`
- timezone: `Asia/Makassar`

## Environment file
Create `/etc/finder-v1.env` with:

```env
APIFY_API_TOKEN=
ANTHROPIC_API_KEY=
YOUTUBE_API_KEY=
SMARTLEAD_API_KEY=
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
FINDER_TIMEZONE=Asia/Makassar
FINDER_OUTPUT_BUCKET=finder-outputs
```

`ANTHROPIC_API_KEY` is optional.

## Initial setup
1. Install base packages:
   - `git`
   - `python3`
   - `python3-venv`
2. Set the server timezone to `Asia/Makassar`.
3. Create the `finder` user if it does not already exist.
4. Clone the worker repo into `/opt/finder-v1`.
5. Create the virtual environment:
   - `python3 -m venv /opt/finder-v1/.venv`
6. Activate the venv and run a smoke check:
   - `/opt/finder-v1/.venv/bin/python -m finder_v1.main doctor`
7. Copy the systemd files from `deploy/systemd/` into `/etc/systemd/system/`.
8. Reload systemd and enable:
   - `finder-v1-repair-on-boot.service`
   - `finder-v1-daily-run.timer`
   - `finder-v1-doc-harvest.timer`
   - `finder-v1-smartlead-reconcile.timer`

## What should happen
- at boot: stale interrupted local state is repaired
- at 02:00 Bali time: the daily run starts
- every 30 minutes: pending DOC jobs are checked
- every 10 minutes: pending Smartlead confirmations are reconciled
- the worker writes plain status to Supabase
- daily CSV/JSON outputs upload to the `finder-outputs` storage bucket

## Basic smoke tests
1. `python -m finder_v1.main doctor`
2. `python -m finder_v1.main status`
3. `python -m finder_v1.main repair-state`
4. `systemctl list-timers --all | grep finder-v1`
5. `systemctl start finder-v1-daily-run.service`
6. `systemctl start finder-v1-smartlead-reconcile.service`

## Expected outputs
- local:
  - `/opt/finder-v1/finder_v1/state/finder.sqlite3`
  - `/opt/finder-v1/finder_v1/output/...`
- cloud:
  - `finder-v1/daily/<day>/run_<id>_review.csv`
  - `finder-v1/daily/<day>/run_<id>_results.csv`
  - `finder-v1/daily/<day>/run_<id>_audit.csv`
  - `finder-v1/daily/<day>/daily_<day>_summary.json`
  - `finder-v1/daily/<day>/daily_<day>_seed_report.csv`
