-- 011_reply_inbox_body_html.sql
-- Adds missing body_html column. Webhook handler inserts both body_text and body_html
-- for inbound replies; without this column the insert silently fails with PGRST204.

ALTER TABLE public.heatr_reply_inbox
    ADD COLUMN IF NOT EXISTS body_html TEXT;

COMMENT ON COLUMN public.heatr_reply_inbox.body_html IS
  'HTML body of the inbound reply, as forwarded by Warmr webhook. NULL voor plain-text replies.';
