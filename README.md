# Finder V1 Worker

This repo is the worker-only cloud deployment for `finder_v1`.

It contains:
- the Python lead-generation worker
- the Supabase migration for Smartlead tracking
- systemd files for the DigitalOcean VM
- simple cloud deployment docs

It does not contain:
- the old frontend dashboard
- the old mixed outreach app code
- local output files
- local SQLite state

## Main commands

Run the cloud readiness check:

```bash
python3 -m finder_v1.main doctor
```

Run the daily worker:

```bash
python3 -m finder_v1.main daily-run --target-emails 100 --hard-stop-hour-local 22
```

Run the DOC harvest:

```bash
python3 -m finder_v1.main check-doc --limit 100
```

## Repo layout

- `finder_v1/`
- `supabase/migrations/003_smartlead_tracking.sql`
- `deploy/systemd/`
- `docs/cloud-deploy.md`
