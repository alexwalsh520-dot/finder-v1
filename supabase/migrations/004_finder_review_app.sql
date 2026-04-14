alter table leads
  add column if not exists review_status text default 'unreviewed',
  add column if not exists reviewed_at timestamptz,
  add column if not exists reviewed_by text,
  add column if not exists review_notes text,
  add column if not exists exported_at timestamptz,
  add column if not exists export_batch_id uuid;

update leads
set review_status = case
  when sent_to_smartlead is true
    or smartlead_sent_at is not null
    or smartlead_campaign_id is not null
    then 'approved'
  else coalesce(review_status, 'unreviewed')
end
where review_status is null
   or (
     review_status = 'unreviewed'
     and (
       sent_to_smartlead is true
       or smartlead_sent_at is not null
       or smartlead_campaign_id is not null
     )
   );

alter table leads
  alter column review_status set default 'unreviewed';

update leads
set review_status = 'unreviewed'
where review_status is null;

alter table leads
  alter column review_status set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'leads_review_status_check'
  ) then
    alter table leads
      add constraint leads_review_status_check
      check (
        review_status in (
          'unreviewed',
          'va_approved',
          'flagged',
          'approved',
          'rejected',
          'exported_pending_confirmation'
        )
      );
  end if;
end $$;

create index if not exists leads_review_status_idx
  on leads (review_status, sent_to_smartlead, batch_date);

create index if not exists leads_export_batch_idx
  on leads (export_batch_id, exported_at desc);

create table if not exists app_settings (
  key text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

alter table app_settings enable row level security;

create table if not exists lead_review_events (
  id uuid primary key default gen_random_uuid(),
  lead_id uuid not null references leads(id) on delete cascade,
  actor_role text not null,
  actor_identifier text not null,
  action text not null,
  payload jsonb,
  created_at timestamptz not null default now()
);

alter table lead_review_events enable row level security;

create index if not exists lead_review_events_lead_id_created_at_idx
  on lead_review_events (lead_id, created_at desc);

insert into app_settings (key, value)
values ('require_owner_approval', 'true'::jsonb)
on conflict (key) do nothing;
