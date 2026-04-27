create table if not exists discovery_seed_expansions (
  seed_handle text primary key,
  expanded_at timestamptz not null default now(),
  related_found integer not null default 0,
  profiles_checked integer not null default 0,
  leads_saved integer not null default 0,
  doc_jobs_submitted integer not null default 0
);

alter table discovery_seed_expansions enable row level security;

create index if not exists discovery_seed_expansions_expanded_at_idx
  on discovery_seed_expansions (expanded_at desc);
