alter table leads
  add column if not exists sent_to_smartlead boolean default false,
  add column if not exists smartlead_campaign_id text,
  add column if not exists smartlead_sent_at timestamptz;

create index if not exists leads_sent_to_smartlead_idx
  on leads (sent_to_smartlead, batch_date);
