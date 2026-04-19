CREATE TABLE IF NOT EXISTS inbound_webhook_endpoints (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  event TEXT NOT NULL DEFAULT 'product.created',
  endpoint_key TEXT NOT NULL,
  signing_secret TEXT,
  active BOOLEAN NOT NULL DEFAULT true,
  source_label TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (btrim(name) <> ''),
  CHECK (btrim(event) <> ''),
  CHECK (btrim(endpoint_key) <> '')
);

ALTER TABLE inbound_webhook_endpoints
  ADD COLUMN IF NOT EXISTS user_id BIGINT,
  ADD COLUMN IF NOT EXISTS name TEXT,
  ADD COLUMN IF NOT EXISTS event TEXT,
  ADD COLUMN IF NOT EXISTS endpoint_key TEXT,
  ADD COLUMN IF NOT EXISTS signing_secret TEXT,
  ADD COLUMN IF NOT EXISTS active BOOLEAN,
  ADD COLUMN IF NOT EXISTS source_label TEXT,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE inbound_webhook_endpoints
SET
  event = COALESCE(NULLIF(btrim(event), ''), 'product.created'),
  active = COALESCE(active, true),
  created_at = COALESCE(created_at, NOW()),
  updated_at = COALESCE(updated_at, NOW())
WHERE
  event IS NULL
  OR btrim(event) = ''
  OR active IS NULL
  OR created_at IS NULL
  OR updated_at IS NULL;

ALTER TABLE inbound_webhook_endpoints
  ALTER COLUMN event SET DEFAULT 'product.created',
  ALTER COLUMN active SET DEFAULT true,
  ALTER COLUMN active SET NOT NULL,
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN created_at SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE inbound_webhook_endpoints
  DROP CONSTRAINT IF EXISTS inbound_webhook_endpoints_name_check;

ALTER TABLE inbound_webhook_endpoints
  ADD CONSTRAINT inbound_webhook_endpoints_name_check
  CHECK (btrim(name) <> '');

ALTER TABLE inbound_webhook_endpoints
  DROP CONSTRAINT IF EXISTS inbound_webhook_endpoints_event_check;

ALTER TABLE inbound_webhook_endpoints
  ADD CONSTRAINT inbound_webhook_endpoints_event_check
  CHECK (btrim(event) <> '');

ALTER TABLE inbound_webhook_endpoints
  DROP CONSTRAINT IF EXISTS inbound_webhook_endpoints_endpoint_key_check;

ALTER TABLE inbound_webhook_endpoints
  ADD CONSTRAINT inbound_webhook_endpoints_endpoint_key_check
  CHECK (btrim(endpoint_key) <> '');

CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_webhook_endpoints_key
  ON inbound_webhook_endpoints(endpoint_key);

CREATE INDEX IF NOT EXISTS idx_inbound_webhook_endpoints_user
  ON inbound_webhook_endpoints(user_id, id DESC);

CREATE TABLE IF NOT EXISTS inbound_webhook_deliveries (
  id BIGSERIAL PRIMARY KEY,
  endpoint_id BIGINT NOT NULL REFERENCES inbound_webhook_endpoints(id) ON DELETE CASCADE,
  event TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status TEXT NOT NULL DEFAULT 'received',
  payload JSONB,
  payload_summary JSONB,
  queue_item_id BIGINT REFERENCES products_queue(id) ON DELETE SET NULL,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (btrim(event) <> ''),
  CHECK (btrim(status) <> '')
);

ALTER TABLE inbound_webhook_deliveries
  ADD COLUMN IF NOT EXISTS endpoint_id BIGINT,
  ADD COLUMN IF NOT EXISTS event TEXT,
  ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS status TEXT,
  ADD COLUMN IF NOT EXISTS payload JSONB,
  ADD COLUMN IF NOT EXISTS payload_summary JSONB,
  ADD COLUMN IF NOT EXISTS queue_item_id BIGINT,
  ADD COLUMN IF NOT EXISTS error_message TEXT,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE inbound_webhook_deliveries
SET
  received_at = COALESCE(received_at, NOW()),
  status = COALESCE(NULLIF(btrim(status), ''), 'received'),
  created_at = COALESCE(created_at, NOW()),
  updated_at = COALESCE(updated_at, NOW())
WHERE
  received_at IS NULL
  OR status IS NULL
  OR btrim(status) = ''
  OR created_at IS NULL
  OR updated_at IS NULL;

ALTER TABLE inbound_webhook_deliveries
  ALTER COLUMN received_at SET DEFAULT NOW(),
  ALTER COLUMN received_at SET NOT NULL,
  ALTER COLUMN status SET DEFAULT 'received',
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN created_at SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE inbound_webhook_deliveries
  DROP CONSTRAINT IF EXISTS inbound_webhook_deliveries_event_check;

ALTER TABLE inbound_webhook_deliveries
  ADD CONSTRAINT inbound_webhook_deliveries_event_check
  CHECK (btrim(event) <> '');

ALTER TABLE inbound_webhook_deliveries
  DROP CONSTRAINT IF EXISTS inbound_webhook_deliveries_status_check;

ALTER TABLE inbound_webhook_deliveries
  ADD CONSTRAINT inbound_webhook_deliveries_status_check
  CHECK (btrim(status) <> '');

CREATE INDEX IF NOT EXISTS idx_inbound_webhook_deliveries_endpoint
  ON inbound_webhook_deliveries(endpoint_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_inbound_webhook_deliveries_queue_item
  ON inbound_webhook_deliveries(queue_item_id);
