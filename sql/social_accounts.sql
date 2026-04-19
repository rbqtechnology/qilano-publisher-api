CREATE TABLE IF NOT EXISTS social_accounts (
  id BIGSERIAL PRIMARY KEY,
  platform TEXT NOT NULL,
  handle TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  hidden BOOLEAN NOT NULL DEFAULT false,
  is_default BOOLEAN NOT NULL DEFAULT false,
  access_token TEXT,
  refresh_token TEXT,
  token_expires_at TIMESTAMPTZ,
  daily_limit INTEGER NOT NULL DEFAULT 0 CHECK (daily_limit >= 0),
  daily_published INTEGER NOT NULL DEFAULT 0 CHECK (daily_published >= 0),
  last_used_at TIMESTAMPTZ,
  instagram_business_account_id TEXT,
  facebook_page_id TEXT,
  instagram_user_id TEXT,
  raw JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (btrim(platform) <> '')
);

ALTER TABLE social_accounts
  ADD COLUMN IF NOT EXISTS platform TEXT,
  ADD COLUMN IF NOT EXISTS handle TEXT,
  ADD COLUMN IF NOT EXISTS status TEXT,
  ADD COLUMN IF NOT EXISTS hidden BOOLEAN,
  ADD COLUMN IF NOT EXISTS is_default BOOLEAN,
  ADD COLUMN IF NOT EXISTS access_token TEXT,
  ADD COLUMN IF NOT EXISTS refresh_token TEXT,
  ADD COLUMN IF NOT EXISTS token_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS daily_limit INTEGER,
  ADD COLUMN IF NOT EXISTS daily_published INTEGER,
  ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS instagram_business_account_id TEXT,
  ADD COLUMN IF NOT EXISTS facebook_page_id TEXT,
  ADD COLUMN IF NOT EXISTS instagram_user_id TEXT,
  ADD COLUMN IF NOT EXISTS raw JSONB,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE social_accounts
SET
  status = COALESCE(NULLIF(btrim(status), ''), 'active'),
  hidden = COALESCE(hidden, false),
  is_default = COALESCE(is_default, false),
  daily_limit = COALESCE(daily_limit, 0),
  daily_published = COALESCE(daily_published, 0),
  created_at = COALESCE(created_at, NOW()),
  updated_at = COALESCE(updated_at, NOW())
WHERE
  status IS NULL
  OR btrim(status) = ''
  OR hidden IS NULL
  OR is_default IS NULL
  OR daily_limit IS NULL
  OR daily_published IS NULL
  OR created_at IS NULL
  OR updated_at IS NULL;

DELETE FROM social_accounts
WHERE id IN (
  SELECT id
  FROM (
    SELECT
      id,
      ROW_NUMBER() OVER (
        PARTITION BY platform, instagram_business_account_id
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
      ) AS rn
    FROM social_accounts
    WHERE instagram_business_account_id IS NOT NULL
  ) ranked
  WHERE rn > 1
);

DELETE FROM social_accounts
WHERE id IN (
  SELECT id
  FROM (
    SELECT
      id,
      ROW_NUMBER() OVER (
        PARTITION BY platform, facebook_page_id
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
      ) AS rn
    FROM social_accounts
    WHERE instagram_business_account_id IS NULL
      AND facebook_page_id IS NOT NULL
  ) ranked
  WHERE rn > 1
);

DELETE FROM social_accounts
WHERE id IN (
  SELECT id
  FROM (
    SELECT
      id,
      ROW_NUMBER() OVER (
        PARTITION BY platform, lower(handle)
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
      ) AS rn
    FROM social_accounts
    WHERE instagram_business_account_id IS NULL
      AND facebook_page_id IS NULL
      AND handle IS NOT NULL
      AND btrim(handle) <> ''
  ) ranked
  WHERE rn > 1
);

ALTER TABLE social_accounts
  ALTER COLUMN platform SET NOT NULL,
  ALTER COLUMN status SET DEFAULT 'active',
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN hidden SET DEFAULT false,
  ALTER COLUMN hidden SET NOT NULL,
  ALTER COLUMN is_default SET DEFAULT false,
  ALTER COLUMN is_default SET NOT NULL,
  ALTER COLUMN daily_limit SET DEFAULT 0,
  ALTER COLUMN daily_limit SET NOT NULL,
  ALTER COLUMN daily_published SET DEFAULT 0,
  ALTER COLUMN daily_published SET NOT NULL,
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN created_at SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE social_accounts
  DROP CONSTRAINT IF EXISTS social_accounts_platform_check;

ALTER TABLE social_accounts
  ADD CONSTRAINT social_accounts_platform_check
  CHECK (btrim(platform) <> '');

ALTER TABLE social_accounts
  DROP CONSTRAINT IF EXISTS social_accounts_daily_limit_check;

ALTER TABLE social_accounts
  ADD CONSTRAINT social_accounts_daily_limit_check
  CHECK (daily_limit >= 0);

ALTER TABLE social_accounts
  DROP CONSTRAINT IF EXISTS social_accounts_daily_published_check;

ALTER TABLE social_accounts
  ADD CONSTRAINT social_accounts_daily_published_check
  CHECK (daily_published >= 0);

DROP INDEX IF EXISTS idx_social_accounts_user_platform;
DROP INDEX IF EXISTS idx_social_accounts_unique_instagram_business;
DROP INDEX IF EXISTS idx_social_accounts_unique_facebook_page;
DROP INDEX IF EXISTS idx_social_accounts_unique_handle;
DROP INDEX IF EXISTS idx_social_accounts_one_default_per_platform;

CREATE INDEX IF NOT EXISTS idx_social_accounts_platform
  ON social_accounts(platform);

CREATE INDEX IF NOT EXISTS idx_social_accounts_platform_status_hidden
  ON social_accounts(platform, status, hidden);

CREATE UNIQUE INDEX IF NOT EXISTS idx_social_accounts_unique_instagram_business
  ON social_accounts(platform, instagram_business_account_id)
  WHERE instagram_business_account_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_social_accounts_unique_facebook_page
  ON social_accounts(platform, facebook_page_id)
  WHERE instagram_business_account_id IS NULL
    AND facebook_page_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_social_accounts_unique_handle
  ON social_accounts(platform, lower(handle))
  WHERE instagram_business_account_id IS NULL
    AND facebook_page_id IS NULL
    AND handle IS NOT NULL
    AND btrim(handle) <> '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_social_accounts_one_default_per_platform
  ON social_accounts(platform)
  WHERE is_default = true;
