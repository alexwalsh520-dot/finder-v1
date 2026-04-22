create index if not exists lead_review_events_actor_history_idx
  on lead_review_events (actor_role, actor_identifier, created_at desc)
  where action in ('qualified', 'not_qualified', 'save');

create index if not exists lead_review_events_owner_history_idx
  on lead_review_events (actor_role, actor_identifier, created_at desc)
  where action in ('owner_approve', 'reject');
