ALTER TABLE mobile_events ADD COLUMN col_hash text;

UPDATE mobile_events
SET col_hash = MD5(ROW(anonymous_id,business_zoned,context_app_version,context_device_id,context_device_id,context_os_name,event_name,received_at,created_at)::text)
WHERE col_hash IS NULL;

CREATE INDEX if not exists idx_sort_id ON mobile_events (lower(anonymous_id), lower(context_device_id) , created_at);

CREATE INDEX if not exists idx_col_hash ON mobile_events (col_hash);

