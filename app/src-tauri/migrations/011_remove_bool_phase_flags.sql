-- Remove redundant scope_finalized and plan_finalized settings keys.
-- app_phase is now the single source of truth for lifecycle state.
DELETE FROM settings WHERE key IN ('scope_finalized', 'plan_finalized');
