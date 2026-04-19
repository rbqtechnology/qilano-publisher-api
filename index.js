// index.js



import express from "express";

import dotenv from "dotenv";

import axios from "axios";

import bcrypt from "bcryptjs";

import crypto from "crypto";

import fs from "fs/promises";
import net from "net";

import jwt from "jsonwebtoken";

import path from "path";

import pkg from "pg";

import { fileURLToPath } from "url";
import ExternalProductImportService from "./services/externalProductImportService.js";
import InternalRepostScheduler from "./services/internalRepostScheduler.js";
import InternalRepostFlowRunner from "./services/internalRepostFlowRunner.js";



dotenv.config();

const { Pool } = pkg;

const __filename = fileURLToPath(import.meta.url);

const __dirname = path.dirname(__filename);



const app = express();

app.use(express.json({ limit: "2mb" }));
const queueUpdateTextParser = express.text({ type: "text/plain", limit: "2mb" });

const DEFAULT_META_GRAPH_VERSION = "v22.0";

const DEFAULT_META_TIMEOUT_MS = 15000;

const DEFAULT_META_CONTAINER_POLL_INTERVAL_MS = 3000;

const DEFAULT_META_CONTAINER_POLL_MAX_ATTEMPTS = 10;
const INSTAGRAM_PHOTO_ASPECT_RATIO_MIN = 4 / 5;
const INSTAGRAM_PHOTO_ASPECT_RATIO_MAX = 1.91;
const INSTAGRAM_IMAGE_METADATA_TIMEOUT_MS = 15000;
const INSTAGRAM_SAFE_CAPTION_MAX_LENGTH = 2000;
const INSTAGRAM_SAFE_DESCRIPTION_SNIPPET_LENGTH = 280;
const DEFAULT_PUBLIC_MEDIA_PATH_PREFIX = "/media";
const DEFAULT_MEDIA_CACHE_DIR = path.join(__dirname, "media");

const DEFAULT_QUEUE_MAX_REPUBLISH = 150;

const DEFAULT_WEBHOOK_EVENT = "product.created";

const REPOST_DELAY_SEQUENCE_HOURS = [6, 24, 48];

const SOCIAL_ACCOUNT_SELECT_FIELDS = `
  id,
  platform,
  handle,
  status,
  hidden,
  is_default,
  token_expires_at,
  daily_limit,
  daily_published,
  last_used_at,
  instagram_business_account_id,
  facebook_page_id,
  instagram_user_id,
  created_at,
  updated_at,
  CASE
    WHEN access_token IS NOT NULL AND length(access_token) > 0 THEN true
    ELSE false
  END AS has_access_token,
  CASE
    WHEN refresh_token IS NOT NULL AND length(refresh_token) > 0 THEN true
    ELSE false
  END AS has_refresh_token,
  CASE
    WHEN raw IS NOT NULL THEN true
    ELSE false
  END AS has_raw
`;

const externalProductImportService = new ExternalProductImportService();



// ---- PostgreSQL Pool ----



const pool = new Pool({

  host: process.env.PGHOST,

  port: Number(process.env.PGPORT || 5432),

  database: process.env.PGDATABASE,

  user: process.env.PGUSER,

  password: process.env.PGPASSWORD,

  ssl:

    process.env.PGSSLMODE === "require"

      ? { rejectUnauthorized: false }

      : false,

});



// ---- JWT ----



const JWT_SECRET = process.env.JWT_SECRET || "<JWT_SECRET>";

const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || "7d";

const QILANO_API_TOKEN = (process.env.QILANO_API_TOKEN || "").trim();
const DEBUG_QUEUE_AUTH = /^(1|true|yes|on)$/i.test(
  String(process.env.DEBUG_QUEUE_AUTH || "").trim()
);
const ENV_USE_INTERNAL_CLAIM = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_CLAIM || "").trim()
);
const ENV_USE_INTERNAL_STATUS_UPDATES = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_STATUS_UPDATES || "").trim()
);
const ENV_USE_INTERNAL_PREPARE_REPOST = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_PREPARE_REPOST || "").trim()
);
const ENV_USE_INTERNAL_PUBLISH_PAYLOAD = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_PUBLISH_PAYLOAD || "").trim()
);
const ENV_USE_INTERNAL_PUBLISH = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_PUBLISH || "").trim()
);
const ENV_USE_INTERNAL_PUBLISH_MARK_STATUS = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_PUBLISH_MARK_STATUS || "").trim()
);
const ENV_USE_INTERNAL_REWRITE = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_REWRITE || "").trim()
);
const ENV_USE_INTERNAL_REWRITE_FOR_PUBLISH = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_REWRITE_FOR_PUBLISH || "").trim()
);
const ENV_USE_INTERNAL_SCHEDULER = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_SCHEDULER || "").trim()
);
const ENV_USE_INTERNAL_FULL_REPOST_FLOW = /^(1|true|yes|on)$/i.test(
  String(process.env.USE_INTERNAL_FULL_REPOST_FLOW || "").trim()
);
const DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1";
const DEFAULT_OPENROUTER_MODEL = "openai/gpt-4o-mini";
const DEFAULT_OPENROUTER_TIMEOUT_MS = 30000;
const DEFAULT_INTERNAL_SCHEDULER_INTERVAL_MS = 60000;
const DEFAULT_INTERNAL_SCHEDULER_MAX_RUNS_PER_TICK = 1;
const DEFAULT_INTERNAL_SCHEDULER_CONCURRENCY = 1;
const DEFAULT_INTERNAL_SCHEDULER_WORKER_ID = "internal-repost-scheduler";
const DEFAULT_INTERNAL_REPOST_LEASE_SECONDS = 60;
const DEFAULT_INTERNAL_REPOST_RETRY_SECONDS = 30;
const DEFAULT_INTERNAL_REPOST_MAX_RETRY_SECONDS = 1800;
const INTERNAL_REPOST_ACTIVITY_LIMIT = 100;
const INTERNAL_REPOST_SETTINGS_TABLE = "app_runtime_settings";
const INTERNAL_REPOST_SETTINGS_PREFIX = "internal_repost.";
const OPENROUTER_SETTINGS_PREFIX = "openrouter.";
const INTERNAL_REPOST_MODE_SHADOW = "shadow";
const INTERNAL_REPOST_MODE_INTERNAL_PRIMARY = "internal_primary";
const INTERNAL_REPOST_MODE_SETTING_KEY = `${INTERNAL_REPOST_SETTINGS_PREFIX}mode`;
const INTERNAL_REPOST_FEATURE_FLAG_DEFINITIONS = [
  {
    envKey: "USE_INTERNAL_CLAIM",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_claim`,
    responseKey: "use_internal_claim",
    defaultValue: ENV_USE_INTERNAL_CLAIM,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_STATUS_UPDATES",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_status_updates`,
    responseKey: "use_internal_status_updates",
    defaultValue: ENV_USE_INTERNAL_STATUS_UPDATES,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_PREPARE_REPOST",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_prepare_repost`,
    responseKey: "use_internal_prepare_repost",
    defaultValue: ENV_USE_INTERNAL_PREPARE_REPOST,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_PUBLISH_PAYLOAD",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_publish_payload`,
    responseKey: "use_internal_publish_payload",
    defaultValue: ENV_USE_INTERNAL_PUBLISH_PAYLOAD,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_PUBLISH",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_publish`,
    responseKey: "use_internal_publish",
    defaultValue: ENV_USE_INTERNAL_PUBLISH,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_PUBLISH_MARK_STATUS",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_publish_mark_status`,
    responseKey: "use_internal_publish_mark_status",
    defaultValue: ENV_USE_INTERNAL_PUBLISH_MARK_STATUS,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_REWRITE",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_rewrite`,
    responseKey: "use_internal_rewrite",
    defaultValue: ENV_USE_INTERNAL_REWRITE,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_REWRITE_FOR_PUBLISH",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_rewrite_for_publish`,
    responseKey: "use_internal_rewrite_for_publish",
    defaultValue: ENV_USE_INTERNAL_REWRITE_FOR_PUBLISH,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_SCHEDULER",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_scheduler`,
    responseKey: "use_internal_scheduler",
    defaultValue: ENV_USE_INTERNAL_SCHEDULER,
    editable: true,
  },
  {
    envKey: "USE_INTERNAL_FULL_REPOST_FLOW",
    settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}use_internal_full_repost_flow`,
    responseKey: "use_internal_full_repost_flow",
    defaultValue: ENV_USE_INTERNAL_FULL_REPOST_FLOW,
    editable: true,
  },
];
const runtimeSettingsState = {
  loaded: false,
  values: new Map(),
  lastLoadedAt: null,
};
const internalRepostActivity = [];



function signToken(user) {

  return jwt.sign({ id: user.id, email: user.email }, JWT_SECRET, {

    expiresIn: JWT_EXPIRES_IN,

  });

}



function safeUser(row) {

  return { id: row.id, email: row.email, created_at: row.created_at };

}

function hasOwn(object, key) {

  return Object.prototype.hasOwnProperty.call(object || {}, key);

}

function logQueueAuth(details) {
  if (!DEBUG_QUEUE_AUTH) {
    return;
  }

  console.info("[queue-auth]", details);
}

function parseAuthorizationHeader(req) {
  const header = String(req.headers.authorization || "");
  const trimmedHeader = header.trim();

  if (!trimmedHeader) {
    return {
      exists: false,
      scheme: "",
      token: "",
      tokenLength: 0,
    };
  }

  const match = trimmedHeader.match(/^(\S+)\s+(.+)$/);
  const scheme = match?.[1] || "";
  const token = (match?.[2] || "").trim();

  return {
    exists: true,
    scheme,
    token,
    tokenLength: token.length,
  };
}

function normalizeOptionalPositiveIntegerString(value) {
  if (value == null || value === "") {
    return null;
  }

  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function normalizeRequestBodyObject(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value;
  }

  if (typeof value !== "string") {
    return {};
  }

  const normalized = value.trim();

  if (!normalized) {
    return {};
  }

  try {
    const parsed = JSON.parse(normalized);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function resolveQueueUserId(req) {
  const requestBody = normalizeRequestBodyObject(req.body);
  const candidates = [
    req.headers["x-qilano-user-id"],
    req.headers["x-user-id"],
    req.query?.user_id,
    requestBody.user_id,
  ];

  for (const candidate of candidates) {
    const userId = normalizeOptionalPositiveIntegerString(candidate);
    if (userId != null) {
      return userId;
    }
  }

  return null;
}

function normalizeOptionalString(value) {

  if (value == null) {

    return null;

  }

  const normalized = String(value).trim();
  return normalized ? normalized : null;

}

function normalizePublicBaseUrl(value) {

  const normalized = normalizeOptionalString(value);

  if (!normalized) {

    return null;

  }

  return normalized.replace(/\/+$/, "");

}

function normalizeLocalDirectoryPath(value) {

  const normalized = normalizeOptionalString(value);

  if (!normalized) {

    return null;

  }

  return path.resolve(normalized);

}

function getConfiguredMediaCacheDirectory() {

  return (
    normalizeLocalDirectoryPath(
      process.env.PUBLIC_MEDIA_STORAGE_DIR ||
        process.env.MEDIA_STORAGE_DIR ||
        process.env.INSTAGRAM_MEDIA_STORAGE_DIR
    ) || DEFAULT_MEDIA_CACHE_DIR
  );

}

function normalizePublicPathPrefix(value) {

  const normalized = normalizeOptionalString(value);

  if (!normalized) {

    return DEFAULT_PUBLIC_MEDIA_PATH_PREFIX;

  }

  const trimmed = normalized.replace(/^\/+/, "").replace(/\/+$/, "");
  return trimmed ? `/${trimmed}` : "";

}

function getConfiguredPublicBaseUrl() {

  return normalizePublicBaseUrl(
    process.env.PUBLIC_BASE_URL ||
      process.env.APP_BASE_URL ||
      process.env.WEBHOOK_PUBLIC_BASE_URL ||
      process.env.API_BASE
  );

}

function getConfiguredPublicMediaBaseUrl() {

  const explicitMediaBaseUrl = normalizePublicBaseUrl(
    process.env.PUBLIC_MEDIA_BASE_URL ||
      process.env.MEDIA_PUBLIC_BASE_URL ||
      process.env.INSTAGRAM_PUBLIC_MEDIA_BASE_URL
  );

  if (explicitMediaBaseUrl) {

    return explicitMediaBaseUrl;

  }

  const publicBaseUrl = getConfiguredPublicBaseUrl();

  if (!publicBaseUrl) {

    return null;

  }

  return `${publicBaseUrl}${normalizePublicPathPrefix(
    process.env.PUBLIC_MEDIA_PATH_PREFIX ||
      process.env.MEDIA_PUBLIC_PATH_PREFIX ||
      process.env.INSTAGRAM_MEDIA_PATH_PREFIX
  )}`;

}

function encodeUrlPathSegments(value) {

  return String(value || "")
    .split("/")
    .filter((segment) => segment.length > 0)
    .map((segment) => encodeURIComponent(segment))
    .join("/");

}

function isPublicHostname(hostname) {

  const normalizedHostname = String(hostname || "")
    .trim()
    .replace(/^\[|\]$/g, "")
    .toLowerCase();

  if (!normalizedHostname) {

    return false;

  }

  if (
    normalizedHostname === "localhost" ||
    normalizedHostname.endsWith(".local") ||
    normalizedHostname.endsWith(".internal")
  ) {

    return false;

  }

  const ipVersion = net.isIP(normalizedHostname);

  if (ipVersion === 4) {

    const octets = normalizedHostname.split(".").map((part) => Number(part));
    const [firstOctet, secondOctet] = octets;

    if (
      firstOctet === 0 ||
      firstOctet === 10 ||
      firstOctet === 127 ||
      (firstOctet === 169 && secondOctet === 254) ||
      (firstOctet === 172 && secondOctet >= 16 && secondOctet <= 31) ||
      (firstOctet === 192 && secondOctet === 168)
    ) {

      return false;

    }

    return true;

  }

  if (ipVersion === 6) {

    if (
      normalizedHostname === "::1" ||
      normalizedHostname === "::" ||
      normalizedHostname.startsWith("fc") ||
      normalizedHostname.startsWith("fd") ||
      normalizedHostname.startsWith("fe80:")
    ) {

      return false;

    }

    return true;

  }

  return normalizedHostname.includes(".");

}

function isPublicHttpUrl(value) {

  try {

    const parsed = new URL(String(value || ""));

    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {

      return false;

    }

    return isPublicHostname(parsed.hostname);

  } catch {

    return false;

  }

}

function normalizeInstagramMediaUrl(value) {

  const normalizedValue = normalizeOptionalString(value);

  if (!normalizedValue) {

    return {
      ok: false,
      error: "image_url is required",
    };

  }

  let normalizedImageUrl = null;

  try {

    if (/^https?:\/\//i.test(normalizedValue)) {

      normalizedImageUrl = new URL(normalizedValue).toString();

    } else if (/^[a-z][a-z0-9+.-]*:/i.test(normalizedValue)) {

      return {
        ok: false,
        error: "image_url must use http or https",
      };

    } else if (normalizedValue.startsWith("/")) {

      const publicBaseUrl = getConfiguredPublicBaseUrl();

      if (!publicBaseUrl) {

        return {
          ok: false,
          error: "image_url must be an absolute public URL when PUBLIC_BASE_URL is not configured",
        };

      }

      normalizedImageUrl = new URL(normalizedValue, `${publicBaseUrl}/`).toString();

    } else {

      const publicMediaBaseUrl = getConfiguredPublicMediaBaseUrl();

      if (!publicMediaBaseUrl) {

        return {
          ok: false,
          error:
            "image_url must be an absolute public URL when PUBLIC_MEDIA_BASE_URL or PUBLIC_BASE_URL is not configured",
        };

      }

      const encodedRelativePath = encodeUrlPathSegments(
        normalizedValue.replace(/^\.\/+/, "").replace(/^\/+/, "")
      );

      if (!encodedRelativePath) {

        return {
          ok: false,
          error: "image_url is required",
        };

      }

      normalizedImageUrl = new URL(
        encodedRelativePath,
        `${publicMediaBaseUrl.replace(/\/+$/, "")}/`
      ).toString();

    }

  } catch {

    return {
      ok: false,
      error: "image_url must be a valid http or https URL",
    };

  }

  if (!isPublicHttpUrl(normalizedImageUrl)) {

    return {
      ok: false,
      error: "image_url must be a public http or https URL",
    };

  }

  return {
    ok: true,
    value: normalizedImageUrl,
  };

}

function normalizeRequestedMediaFilename(value) {

  const normalized = normalizeOptionalString(value);

  if (!normalized) {

    return null;

  }

  const basename = path.basename(normalized);
  return basename === normalized ? basename : null;

}

function normalizeWebhookImageUrl(value) {

  const normalized = normalizeOptionalString(value);

  if (!normalized) {

    return null;

  }

  if (/^https?:\/\//i.test(normalized)) {

    return normalized;

  }

  if (normalized.startsWith("//")) {

    return `https:${normalized}`;

  }

  return null;

}

function extractImageUrlsFromHtml(value) {

  const source = String(value || "");

  if (!source) {

    return [];

  }

  const seen = new Set();
  const output = [];
  const pattern = /<img\b[^>]*\bsrc\s*=\s*["']([^"'#?][^"']*)["'][^>]*>/gi;
  let match = null;

  while ((match = pattern.exec(source))) {

    const normalizedUrl = normalizeWebhookImageUrl(match[1]);

    if (!normalizedUrl || seen.has(normalizedUrl)) {

      continue;

    }

    seen.add(normalizedUrl);
    output.push(normalizedUrl);

  }

  return output;

}

async function fileExists(value) {

  try {

    await fs.access(value);
    return true;

  } catch {

    return false;

  }

}

async function resolveCachedMediaFilePath(filename) {

  const normalizedFilename = normalizeRequestedMediaFilename(filename);

  if (!normalizedFilename) {

    return null;

  }

  const mediaCacheDirectory = getConfiguredMediaCacheDirectory();
  const candidatePaths = [
    path.join(mediaCacheDirectory, normalizedFilename),
    path.join(mediaCacheDirectory, "product", normalizedFilename),
    path.join(mediaCacheDirectory, "storage", "product", normalizedFilename),
  ];

  for (const candidatePath of candidatePaths) {

    if (await fileExists(candidatePath)) {

      return candidatePath;

    }

  }

  return null;

}

async function findWebhookBackedMediaSource(filename) {

  const normalizedFilename = normalizeRequestedMediaFilename(filename);

  if (!normalizedFilename) {

    return null;

  }

  const query = `
    SELECT id, payload
    FROM inbound_webhook_deliveries
    WHERE EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(payload->'product'->'images') = 'array'
            THEN payload->'product'->'images'
          ELSE '[]'::jsonb
        END
      ) AS image(value)
      WHERE image.value = $1
    )
    ORDER BY id DESC
    LIMIT 5
  `;

  const result = await pool.query(query, [normalizedFilename]);

  for (const row of result.rows) {

    const product = row?.payload?.product;
    const images = Array.isArray(product?.images)
      ? product.images
          .map((value) => normalizeRequestedMediaFilename(value))
          .filter(Boolean)
      : [];
    const imageIndex = images.indexOf(normalizedFilename);

    if (imageIndex === -1) {

      continue;

    }

    const detailImageUrls = extractImageUrlsFromHtml(product?.details);
    const sourceUrl = detailImageUrls[imageIndex] || detailImageUrls[0] || null;

    if (!sourceUrl) {

      continue;

    }

    return {
      deliveryId: row.id,
      filename: normalizedFilename,
      sourceUrl,
      inferredFromIndex: detailImageUrls[imageIndex] ? imageIndex : 0,
    };

  }

  return null;

}

async function cacheMediaFileFromWebhookSource(filename) {

  const normalizedFilename = normalizeRequestedMediaFilename(filename);

  if (!normalizedFilename) {

    return null;

  }

  const mediaCacheDirectory = getConfiguredMediaCacheDirectory();
  const destinationPath = path.join(mediaCacheDirectory, normalizedFilename);

  if (await fileExists(destinationPath)) {

    return destinationPath;

  }

  const source = await findWebhookBackedMediaSource(normalizedFilename);

  if (!source?.sourceUrl) {

    return null;

  }

  const response = await axios.get(source.sourceUrl, {
    responseType: "arraybuffer",
    timeout: 15000,
    maxRedirects: 5,
    validateStatus: (status) => status >= 200 && status < 400,
  });

  const contentType = normalizeOptionalString(response.headers?.["content-type"]) || "";

  if (!contentType.toLowerCase().startsWith("image/")) {

    throw new Error(
      `Upstream media source returned non-image content-type for ${normalizedFilename}`
    );

  }

  await fs.mkdir(mediaCacheDirectory, { recursive: true });
  await fs.writeFile(destinationPath, response.data);
  return destinationPath;

}

function decodeHtmlEntities(value) {

  const source = String(value || "");
  if (!source) {
    return "";
  }

  const namedEntities = {
    amp: "&",
    lt: "<",
    gt: ">",
    quot: "\"",
    apos: "'",
    nbsp: " ",
    ndash: "-",
    mdash: "-",
    hellip: "...",
  };

  return source.replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (match, entity) => {
    const normalizedEntity = String(entity || "").toLowerCase();

    if (normalizedEntity.startsWith("#x")) {
      const parsed = Number.parseInt(normalizedEntity.slice(2), 16);
      return Number.isFinite(parsed) ? String.fromCodePoint(parsed) : match;
    }

    if (normalizedEntity.startsWith("#")) {
      const parsed = Number.parseInt(normalizedEntity.slice(1), 10);
      return Number.isFinite(parsed) ? String.fromCodePoint(parsed) : match;
    }

    return namedEntities[normalizedEntity] ?? match;
  });

}

function stripHtmlToPlainText(value) {

  const source = String(value || "");
  if (!source) {
    return "";
  }

  return source
    .replace(/<\s*br\s*\/?>/gi, "\n")
    .replace(/<\s*\/p\s*>/gi, "\n\n")
    .replace(/<\s*\/div\s*>/gi, "\n")
    .replace(/<\s*\/li\s*>/gi, "\n")
    .replace(/<li\b[^>]*>/gi, "- ")
    .replace(/<[^>]+>/g, " ");

}

function normalizeCaptionWhitespace(value) {

  const source = String(value || "");
  if (!source) {
    return "";
  }

  return source
    .replace(/\r\n?/g, "\n")
    .replace(/[ \t\f\v\u00a0]+/g, " ")
    .replace(/ *\n */g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

}

function sanitizeInstagramCaptionText(value) {

  return normalizeCaptionWhitespace(decodeHtmlEntities(stripHtmlToPlainText(value)));

}

function normalizeInstagramHashtags(value) {

  const source =
    typeof value === "string"
      ? value
      : Array.isArray(value)
        ? value.join(" ")
        : "";
  const sanitized = sanitizeInstagramCaptionText(source);
  if (!sanitized) {
    return "";
  }

  const matches = sanitized.match(/#[\p{L}\p{N}_]+/gu);
  return matches ? matches.join(" ") : "";

}

function truncateCaptionPart(value, maxLength) {

  const source = String(value || "").trim();
  if (!source || !Number.isInteger(maxLength) || maxLength <= 0) {
    return "";
  }

  if (source.length <= maxLength) {
    return source;
  }

  const ellipsis = "...";
  const hardLimit = Math.max(0, maxLength - ellipsis.length);
  let truncated = source.slice(0, hardLimit).trim();
  const lastBreak = Math.max(
    truncated.lastIndexOf("\n"),
    truncated.lastIndexOf(" "),
    truncated.lastIndexOf("."),
    truncated.lastIndexOf(",")
  );

  if (lastBreak >= Math.floor(hardLimit * 0.6)) {
    truncated = truncated.slice(0, lastBreak).trim();
  }

  return `${truncated}${ellipsis}`.slice(0, maxLength);

}

function buildInstagramSafeCaption({
  caption,
  title,
  description,
  hashtags,
  maxLength = INSTAGRAM_SAFE_CAPTION_MAX_LENGTH,
} = {}) {

  const sanitizedCaption = sanitizeInstagramCaptionText(caption);
  const sanitizedTitle = sanitizeInstagramCaptionText(title);
  const sanitizedDescription = sanitizeInstagramCaptionText(description);
  const sanitizedHashtags = normalizeInstagramHashtags(hashtags);

  const parts = [];
  const usedValues = new Set();
  const addPart = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) {
      return;
    }

    const dedupeKey = normalized.toLowerCase();
    if (usedValues.has(dedupeKey)) {
      return;
    }

    usedValues.add(dedupeKey);
    parts.push(normalized);
  };

  addPart(sanitizedTitle);

  const descriptionSource =
    sanitizedCaption && sanitizedCaption.toLowerCase() !== (sanitizedTitle || "").toLowerCase()
      ? sanitizedCaption
      : sanitizedDescription;
  const descriptionSnippet = truncateCaptionPart(
    descriptionSource,
    INSTAGRAM_SAFE_DESCRIPTION_SNIPPET_LENGTH
  );
  addPart(descriptionSnippet);

  const separatorLength = parts.length > 0 ? parts.length * 2 : 0;
  const remainingForHashtags = Math.max(0, maxLength - separatorLength - parts.join("").length);
  const hashtagSnippet = truncateCaptionPart(sanitizedHashtags, remainingForHashtags);
  addPart(hashtagSnippet);

  const composed = parts.join("\n\n").trim();
  const finalCaption = truncateCaptionPart(composed, maxLength);

  return {
    caption: finalCaption,
    caption_length: finalCaption.length,
    was_truncated:
      finalCaption.length < composed.length ||
      descriptionSnippet.length < descriptionSource.length ||
      hashtagSnippet.length < sanitizedHashtags.length,
    components: {
      title: sanitizedTitle,
      description_snippet: descriptionSnippet,
      hashtags: hashtagSnippet,
    },
  };

}

function normalizeRequiredPlatform(value) {

  const normalized = normalizeOptionalString(value);
  return normalized ? normalized.toLowerCase() : "";

}

function normalizeOptionalBoolean(value) {

  if (typeof value === "boolean") {

    return value;

  }

  if (typeof value === "number") {

    if (value === 1) return true;
    if (value === 0) return false;

  }

  if (typeof value === "string") {

    const normalized = value.trim().toLowerCase();

    if (["true", "1", "yes", "on"].includes(normalized)) return true;
    if (["false", "0", "no", "off"].includes(normalized)) return false;

  }

  return null;

}

function maskSecret(value) {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (!normalized) {
    return null;
  }

  if (normalized.length <= 8) {
    return `${normalized.slice(0, 2)}***${normalized.slice(-2)}`;
  }

  return `${normalized.slice(0, 4)}***${normalized.slice(-4)}`;
}

function normalizeOptionalNonNegativeInteger(value) {
  const parsed = normalizeOptionalInteger(value);
  return parsed != null && parsed >= 0 ? parsed : null;
}

function getRuntimeSettingRecord(settingKey) {
  return runtimeSettingsState.values.get(settingKey) || null;
}

function getRuntimeSettingValue(settingKey) {
  return getRuntimeSettingRecord(settingKey)?.value;
}

function getRuntimeStringSetting(settingKey, envValue, fallbackValue = "") {
  const savedValue = getRuntimeSettingValue(settingKey);
  if (typeof savedValue === "string") {
    const normalizedSavedValue = savedValue.trim();
    if (normalizedSavedValue) {
      return normalizedSavedValue;
    }
  }

  const normalizedEnvValue = typeof envValue === "string" ? envValue.trim() : "";
  if (normalizedEnvValue) {
    return normalizedEnvValue;
  }

  return fallbackValue;
}

function getRuntimeBooleanSetting(settingKey, envDefault = false) {
  const savedValue = getRuntimeSettingValue(settingKey);
  if (typeof savedValue === "boolean") {
    return savedValue;
  }

  return envDefault === true;
}

function getRuntimeIntegerSetting(settingKey, envValue, fallbackValue, options = {}) {
  const min = Number.isFinite(options.min) ? options.min : Number.NEGATIVE_INFINITY;
  const max = Number.isFinite(options.max) ? options.max : Number.POSITIVE_INFINITY;
  const candidates = [getRuntimeSettingValue(settingKey), envValue, fallbackValue];

  for (const candidate of candidates) {
    const parsed = Number(candidate);
    if (
      Number.isInteger(parsed) &&
      Number.isFinite(parsed) &&
      parsed >= min &&
      parsed <= max
    ) {
      return parsed;
    }
  }

  return fallbackValue;
}

function getInternalRepostFeatureFlags() {
  return INTERNAL_REPOST_FEATURE_FLAG_DEFINITIONS.reduce((accumulator, definition) => {
    accumulator[definition.responseKey] = getRuntimeBooleanSetting(
      definition.settingKey,
      definition.defaultValue
    );
    return accumulator;
  }, {});
}

function normalizeInternalRepostMode(value) {
  const normalized = normalizeOptionalString(value)?.toLowerCase() || "";
  if (!normalized) {
    return null;
  }

  if (
    [
      INTERNAL_REPOST_MODE_SHADOW,
      "workflow_primary",
      "workflow-primary",
      "external_primary",
      "external-primary",
    ].includes(normalized)
  ) {
    return INTERNAL_REPOST_MODE_SHADOW;
  }

  if (
    [
      INTERNAL_REPOST_MODE_INTERNAL_PRIMARY,
      "internal-primary",
      "internal",
      "production_internal",
      "production-internal",
    ].includes(normalized)
  ) {
    return INTERNAL_REPOST_MODE_INTERNAL_PRIMARY;
  }

  return null;
}

function resolveInternalRepostOperatingMode(configuredFeatureFlags) {
  const envMode = normalizeInternalRepostMode(process.env.INTERNAL_REPOST_MODE);
  if (envMode) {
    return {
      mode: envMode,
      source: "env_override",
    };
  }

  const savedMode = normalizeInternalRepostMode(getRuntimeSettingValue(INTERNAL_REPOST_MODE_SETTING_KEY));
  if (savedMode) {
    return {
      mode: savedMode,
      source: "runtime_setting",
    };
  }

  return {
    mode:
      configuredFeatureFlags.use_internal_scheduler ||
      configuredFeatureFlags.use_internal_full_repost_flow
        ? INTERNAL_REPOST_MODE_INTERNAL_PRIMARY
        : INTERNAL_REPOST_MODE_SHADOW,
    source: "derived_from_feature_flags",
  };
}

function getEffectiveInternalRepostFeatureFlags(configuredFeatureFlags, operatingMode) {
  const effectiveFeatureFlags = {
    ...configuredFeatureFlags,
  };

  if (operatingMode === INTERNAL_REPOST_MODE_SHADOW) {
    effectiveFeatureFlags.use_internal_scheduler = false;
    effectiveFeatureFlags.use_internal_full_repost_flow = false;
  }

  return effectiveFeatureFlags;
}

function buildInternalRepostManualActions(configuredFeatureFlags) {
  return {
    refresh_status: {
      available: true,
      label: "Refresh Status",
      mode: "monitoring",
    },
    run_now: {
      available:
        configuredFeatureFlags.use_internal_claim === true &&
        configuredFeatureFlags.use_internal_prepare_repost === true &&
        configuredFeatureFlags.use_internal_publish_payload === true &&
        configuredFeatureFlags.use_internal_publish === true,
      label: "Run Now (Diagnostic)",
      mode: "manual_diagnostic",
    },
    test_rewrite: {
      available: configuredFeatureFlags.use_internal_rewrite === true,
      label: "Test Rewrite",
      mode: "manual_diagnostic",
    },
    test_build_payload: {
      available: configuredFeatureFlags.use_internal_publish_payload === true,
      label: "Test Build Payload",
      mode: "manual_diagnostic",
    },
    test_publish: {
      available: configuredFeatureFlags.use_internal_publish === true,
      label: "Test Publish",
      mode: "manual_diagnostic",
    },
  };
}

function buildInternalRepostModeSummary(runtimeConfig, schedulerStatus = null) {
  const workflowPrimary = runtimeConfig.operatingMode === INTERNAL_REPOST_MODE_SHADOW;
  const autoProcessingEnabled =
    runtimeConfig.featureFlags.use_internal_scheduler === true &&
    runtimeConfig.featureFlags.use_internal_full_repost_flow === true &&
    schedulerStatus?.started === true;

  if (workflowPrimary) {
    return {
      primaryPublishingPath: "workflow",
      internalRepostMode: INTERNAL_REPOST_MODE_SHADOW,
      dashboardMode: "workflow_primary_shadow",
      activeSchedulerPath: "workflow_primary",
      autoProcessingEnabled,
      statusBadges: ["Workflow Primary", "Internal Repost in Shadow Mode"],
      statusMessage:
        "Workflow / n8n is the primary production publishing path. Internal repost is limited to monitoring, shadow testing, and manual diagnostics.",
    };
  }

  return {
    primaryPublishingPath: "internal",
    internalRepostMode: INTERNAL_REPOST_MODE_INTERNAL_PRIMARY,
    dashboardMode: "internal_primary",
    activeSchedulerPath:
      schedulerStatus?.started === true
        ? "internal_scheduler"
        : "internal_scheduler_configured_not_started",
    autoProcessingEnabled,
    statusBadges: ["Internal Repost Primary"],
    statusMessage:
      "Internal repost is configured as the active publishing path. Workflow / n8n should be paused to avoid overlapping claims.",
  };
}

function getInternalRepostRuntimeConfig() {
  const configuredFeatureFlags = getInternalRepostFeatureFlags();
  const operatingModeResolution = resolveInternalRepostOperatingMode(configuredFeatureFlags);
  const featureFlags = getEffectiveInternalRepostFeatureFlags(
    configuredFeatureFlags,
    operatingModeResolution.mode
  );
  const openRouterApiKeySaved = getRuntimeStringSetting(
    `${OPENROUTER_SETTINGS_PREFIX}api_key`,
    "",
    ""
  );
  const openRouterApiKeyEnv = String(process.env.OPENROUTER_API_KEY || "").trim();
  const openRouterApiKey = openRouterApiKeySaved || openRouterApiKeyEnv;
  const openRouterModel = getRuntimeStringSetting(
    `${OPENROUTER_SETTINGS_PREFIX}model`,
    process.env.OPENROUTER_MODEL,
    DEFAULT_OPENROUTER_MODEL
  );
  const openRouterBaseUrl = getRuntimeStringSetting(
    `${OPENROUTER_SETTINGS_PREFIX}base_url`,
    process.env.OPENROUTER_BASE_URL,
    DEFAULT_OPENROUTER_BASE_URL
  );
  const openRouterTimeoutMs = getRuntimeIntegerSetting(
    `${OPENROUTER_SETTINGS_PREFIX}timeout_ms`,
    process.env.OPENROUTER_TIMEOUT_MS,
    DEFAULT_OPENROUTER_TIMEOUT_MS,
    { min: 1000, max: 300000 }
  );
  const workerId = getRuntimeStringSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}worker_id`,
    process.env.INTERNAL_SCHEDULER_WORKER_ID,
    DEFAULT_INTERNAL_SCHEDULER_WORKER_ID
  );
  const leaseSeconds = getRuntimeIntegerSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}lease_seconds`,
    process.env.INTERNAL_REPOST_LEASE_SECONDS,
    DEFAULT_INTERNAL_REPOST_LEASE_SECONDS,
    { min: 10, max: 600 }
  );
  const retrySeconds = getRuntimeIntegerSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}retry_seconds`,
    process.env.INTERNAL_REPOST_RETRY_SECONDS,
    DEFAULT_INTERNAL_REPOST_RETRY_SECONDS,
    { min: 5, max: DEFAULT_INTERNAL_REPOST_MAX_RETRY_SECONDS }
  );
  const schedulerIntervalMs = getRuntimeIntegerSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}scheduler_interval_ms`,
    process.env.INTERNAL_SCHEDULER_INTERVAL_MS,
    DEFAULT_INTERNAL_SCHEDULER_INTERVAL_MS,
    { min: 1000, max: 3600000 }
  );
  const maxRunsPerTick = getRuntimeIntegerSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}max_runs_per_tick`,
    process.env.INTERNAL_SCHEDULER_MAX_RUNS_PER_TICK,
    DEFAULT_INTERNAL_SCHEDULER_MAX_RUNS_PER_TICK,
    { min: 1, max: 100 }
  );
  const concurrency = getRuntimeIntegerSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}concurrency`,
    process.env.INTERNAL_SCHEDULER_CONCURRENCY,
    DEFAULT_INTERNAL_SCHEDULER_CONCURRENCY,
    { min: 1, max: 25 }
  );
  const dryRun = getRuntimeBooleanSetting(
    `${INTERNAL_REPOST_SETTINGS_PREFIX}dry_run`,
    /^(1|true|yes|on)$/i.test(String(process.env.INTERNAL_SCHEDULER_DRY_RUN || "").trim())
  );

  return {
    operatingMode: operatingModeResolution.mode,
    operatingModeSource: operatingModeResolution.source,
    configuredFeatureFlags,
    featureFlags,
    openRouter: {
      apiKey: openRouterApiKey,
      apiKeyConfigured: Boolean(openRouterApiKey),
      apiKeyMasked: maskSecret(openRouterApiKey),
      model: openRouterModel || DEFAULT_OPENROUTER_MODEL,
      baseUrl: openRouterBaseUrl || DEFAULT_OPENROUTER_BASE_URL,
      timeoutMs: openRouterTimeoutMs,
    },
    scheduler: {
      schedulerEnabled: featureFlags.use_internal_scheduler,
      fullFlowEnabled: featureFlags.use_internal_full_repost_flow,
      intervalMs: schedulerIntervalMs,
      maxRunsPerTick,
      concurrency,
      workerId: workerId || DEFAULT_INTERNAL_SCHEDULER_WORKER_ID,
      dryRun,
      leaseSeconds,
      retrySeconds,
    },
  };
}

function normalizeOptionalInteger(value) {

  if (value == null || value === "") {

    return null;

  }

  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : null;

}

function resolveQueueMaxRepublish(value) {

  return Number.isInteger(value) ? value : DEFAULT_QUEUE_MAX_REPUBLISH;

}

function computeNextQueueRunAt(publishCount) {

  const normalizedPublishCount = Number.isInteger(publishCount) && publishCount > 0
    ? publishCount
    : 1;

  const delayHours =
    REPOST_DELAY_SEQUENCE_HOURS[
      Math.min(normalizedPublishCount - 1, REPOST_DELAY_SEQUENCE_HOURS.length - 1)
    ];

  return new Date(Date.now() + delayHours * 60 * 60 * 1000).toISOString();

}

function getQueuePublishDecision(item, options = {}) {

  const requireDueNow = options.requireDueNow !== false;
  const now = options.now instanceof Date ? options.now : new Date();
  const currentPublishCount = Number.isInteger(item?.publish_count) ? item.publish_count : 0;
  const effectiveMaxRepublish = resolveQueueMaxRepublish(item?.max_republish);
  const nextRunAtValue = item?.next_run_at == null ? null : new Date(item.next_run_at);
  const hasValidNextRunAt = nextRunAtValue != null && !Number.isNaN(nextRunAtValue.getTime());
  const isDueNow = !hasValidNextRunAt || nextRunAtValue.getTime() <= now.getTime();
  const allowedStatuses = ["scheduled", "published", "failed"];
  const currentStatus = String(item?.status || "").trim().toLowerCase();
  const hasLifecycleRemaining = currentPublishCount < effectiveMaxRepublish;
  const isProcessing = currentStatus === "processing";
  const eligible =
    hasLifecycleRemaining &&
    !isProcessing &&
    allowedStatuses.includes(currentStatus) &&
    (!requireDueNow || isDueNow);

  return {
    eligible,
    currentPublishCount,
    effectiveMaxRepublish,
    isDueNow,
    nextRunAt: hasValidNextRunAt ? nextRunAtValue.toISOString() : null,
    currentStatus,
    reason: eligible
      ? "eligible"
      : !hasLifecycleRemaining
        ? "lifecycle_exhausted"
        : isProcessing
          ? "already_processing"
          : !allowedStatuses.includes(currentStatus)
            ? "status_not_publishable"
            : "not_due_yet",
  };

}

function normalizeOptionalTimestamp(value) {

  if (value == null || value === "") {

    return null;

  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();

}

function normalizeOptionalJson(value) {

  if (value == null) {

    return null;

  }

  if (typeof value === "string") {

    try {

      return JSON.stringify(JSON.parse(value));

    } catch {

      return JSON.stringify({ value });

    }

  }

  try {

    return JSON.stringify(value);

  } catch {

    return null;

  }

}

function normalizeQueueItemStatus(value) {

  const normalized = normalizeOptionalString(value);

  if (!normalized) {

    return null;

  }

  const lowered = normalized.toLowerCase();
  return ["scheduled", "published", "failed", "processing"].includes(lowered)
    ? lowered
    : null;

}

function normalizeQueueImages(value) {

  if (value == null) {

    return null;

  }

  if (Array.isArray(value)) {

    return value
      .filter((item) => typeof item === "string" && item.trim())
      .map((item) => item.trim());

  }

  if (typeof value === "string") {

    const normalized = value.trim();

    if (!normalized) {

      return null;

    }

    try {

      return normalizeQueueImages(JSON.parse(normalized));

    } catch {

      return [normalized];

    }

  }

  return null;

}

function normalizeQueueImageRotationState(value) {

  if (value == null) {

    return null;

  }

  let parsed = value;

  if (typeof value === "string") {

    try {

      parsed = JSON.parse(value);

    } catch {

      return null;

    }

  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {

    return null;

  }

  const cycleOrder = Array.isArray(parsed.cycle_order)
    ? parsed.cycle_order.filter((item) => Number.isInteger(item) && item >= 0)
    : null;
  const nextPosition = Number.isInteger(parsed.next_position) && parsed.next_position >= 0
    ? parsed.next_position
    : 0;
  const lastSelectedIndex =
    Number.isInteger(parsed.last_selected_index) && parsed.last_selected_index >= 0
      ? parsed.last_selected_index
      : null;

  return cycleOrder
    ? {
        cycle_order: cycleOrder,
        next_position: nextPosition,
        last_selected_index: lastSelectedIndex,
      }
    : null;

}

function shuffleQueueImageIndices(length) {

  const indices = Array.from({ length }, (_, index) => index);

  for (let index = indices.length - 1; index > 0; index -= 1) {

    const swapIndex = Math.floor(Math.random() * (index + 1));
    const currentValue = indices[index];
    indices[index] = indices[swapIndex];
    indices[swapIndex] = currentValue;

  }

  return indices;

}

function buildQueueImageCycleOrder(length) {

  return Array.from({ length }, (_, index) => index);

}

function buildInitialQueueImageRotationState(images) {

  if (!Array.isArray(images) || images.length === 0) {

    return null;

  }

  return {
    cycle_order: buildQueueImageCycleOrder(images.length),
    next_position: 0,
    last_selected_index: null,
  };

}

function getNextQueueImageSelection(item) {

  const images = normalizeQueueImages(item?.images);
  const fallbackImage = normalizeOptionalString(item?.last_image);
  const currentPublishCount = Number.isInteger(item?.publish_count)
    ? item.publish_count
    : 0;

  if (!Array.isArray(images) || images.length === 0) {

    return {
      selectedImage: fallbackImage,
      nextRotationState: null,
      imageCount: 0,
      source: fallbackImage ? "last_image_fallback" : "no_image_available",
    };

  }

  if (images.length === 1) {

    return {
      selectedImage: images[0],
      nextRotationState: {
        cycle_order: [0],
        next_position: 0,
        last_selected_index: 0,
      },
      imageCount: 1,
      source: "single_image",
    };

  }

  const state = normalizeQueueImageRotationState(item?.image_rotation_state);
  const shouldResetToFreshCycle = currentPublishCount === 0;
  const cycleOrder =
    !shouldResetToFreshCycle &&
    Array.isArray(state?.cycle_order) &&
    state.cycle_order.length === images.length
      ? state.cycle_order
      : null;
  const currentCycle =
    cycleOrder &&
    new Set(cycleOrder).size === images.length &&
    cycleOrder.every((index) => index >= 0 && index < images.length)
      ? cycleOrder
      : buildQueueImageCycleOrder(images.length);
  const currentPosition =
    !shouldResetToFreshCycle &&
    cycleOrder &&
    Number.isInteger(state?.next_position)
      ? state.next_position
      : 0;
  const normalizedPosition =
    currentPosition >= 0 && currentPosition < currentCycle.length ? currentPosition : 0;
  const selectedIndex = currentCycle[normalizedPosition] ?? 0;
  const selectedImage = images[selectedIndex] || fallbackImage || images[0];
  const nextPosition = normalizedPosition + 1;
  const nextRotationState =
    nextPosition >= currentCycle.length
      ? {
          cycle_order: buildQueueImageCycleOrder(images.length),
          next_position: 0,
          last_selected_index: selectedIndex,
        }
      : {
          cycle_order: currentCycle,
          next_position: nextPosition,
          last_selected_index: selectedIndex,
        };

  return {
    selectedImage,
    nextRotationState,
    imageCount: images.length,
    source: shouldResetToFreshCycle
      ? "fresh_cycle_reset"
      : cycleOrder
        ? "rotation_state"
        : "new_cycle",
  };

}

function buildQueueImageRotationSelectionContext(item) {
  const images = normalizeQueueImages(item?.images);
  const fallbackImage = normalizeOptionalString(item?.last_image);
  const currentPublishCount = Number.isInteger(item?.publish_count)
    ? item.publish_count
    : 0;

  if (!Array.isArray(images) || images.length === 0) {
    return {
      images: [],
      fallbackImage,
      state: null,
      currentCycle: [],
      normalizedPosition: 0,
      shouldResetToFreshCycle: currentPublishCount === 0,
      source: fallbackImage ? "last_image_fallback" : "no_image_available",
    };
  }

  if (images.length === 1) {
    return {
      images,
      fallbackImage,
      state: {
        cycle_order: [0],
        next_position: 0,
        last_selected_index: 0,
      },
      currentCycle: [0],
      normalizedPosition: 0,
      shouldResetToFreshCycle: currentPublishCount === 0,
      source: "single_image",
    };
  }

  const state = normalizeQueueImageRotationState(item?.image_rotation_state);
  const shouldResetToFreshCycle = currentPublishCount === 0;
  const cycleOrder =
    !shouldResetToFreshCycle &&
    Array.isArray(state?.cycle_order) &&
    state.cycle_order.length === images.length
      ? state.cycle_order
      : null;
  const currentCycle =
    cycleOrder &&
    new Set(cycleOrder).size === images.length &&
    cycleOrder.every((index) => index >= 0 && index < images.length)
      ? cycleOrder
      : buildQueueImageCycleOrder(images.length);
  const currentPosition =
    !shouldResetToFreshCycle &&
    cycleOrder &&
    Number.isInteger(state?.next_position)
      ? state.next_position
      : 0;
  const normalizedPosition =
    currentPosition >= 0 && currentPosition < currentCycle.length ? currentPosition : 0;

  return {
    images,
    fallbackImage,
    state,
    currentCycle,
    normalizedPosition,
    shouldResetToFreshCycle,
    source: shouldResetToFreshCycle
      ? "fresh_cycle_reset"
      : cycleOrder
        ? "rotation_state"
        : "new_cycle",
  };
}

function buildQueueImageRotationStateForSelection(currentCycle, selectedPosition, selectedIndex) {
  if (!Array.isArray(currentCycle) || currentCycle.length === 0) {
    return null;
  }

  const nextPosition = selectedPosition + 1;
  return nextPosition >= currentCycle.length
    ? {
        cycle_order: buildQueueImageCycleOrder(currentCycle.length),
        next_position: 0,
        last_selected_index: selectedIndex,
      }
    : {
        cycle_order: currentCycle,
        next_position: nextPosition,
        last_selected_index: selectedIndex,
      };
}

async function inspectInstagramQueueImageCandidate(imageUrl) {
  const normalizedResult = normalizeInstagramMediaUrl(imageUrl);
  if (!normalizedResult.ok || !normalizedResult.value) {
    return {
      ok: false,
      reason: normalizedResult.error || "image_url is invalid",
      normalizedUrl: null,
      metadata: null,
      aspectRatio: null,
    };
  }

  try {
    const metadata = await fetchImageMetadata(normalizedResult.value);
    const aspect = validateInstagramPhotoAspectRatio(metadata.width, metadata.height);

    if (!aspect.accepted) {
      return {
        ok: false,
        reason: aspect.reason || "Unsupported aspect ratio",
        normalizedUrl: normalizedResult.value,
        metadata,
        aspectRatio: aspect.aspectRatio,
      };
    }

    return {
      ok: true,
      reason: null,
      normalizedUrl: normalizedResult.value,
      metadata,
      aspectRatio: aspect.aspectRatio,
    };
  } catch (error) {
    return {
      ok: false,
      reason: error?.message || "Failed to inspect remote image",
      normalizedUrl: normalizedResult.value,
      metadata: null,
      aspectRatio: null,
    };
  }
}

async function getNextValidQueueImageSelection(item) {
  const selectionContext = buildQueueImageRotationSelectionContext(item);
  const previousImage = normalizeOptionalString(item?.last_image);
  const queueItemId = item?.id ?? null;
  const productId = item?.product_id ?? null;

  if (selectionContext.images.length === 0) {
    console.log("[queue-image-rotation] selection", {
      queue_item_id: queueItemId,
      product_id: productId,
      previous_image: previousImage,
      next_candidate_image: null,
      skipped_invalid_images: [],
      final_selected_image: selectionContext.fallbackImage,
      updated_rotation_state: null,
      image_rotation_source: selectionContext.source,
    });
    return {
      selectedImage: selectionContext.fallbackImage,
      nextRotationState: null,
      imageCount: 0,
      source: selectionContext.source,
      previousImage,
      nextCandidateImage: null,
      skippedInvalidImages: [],
    };
  }

  const skippedInvalidImages = [];
  const firstCandidateIndex =
    selectionContext.currentCycle[selectionContext.normalizedPosition] ?? null;
  const firstCandidateImage =
    firstCandidateIndex != null ? selectionContext.images[firstCandidateIndex] || null : null;

  for (let offset = 0; offset < selectionContext.currentCycle.length; offset += 1) {
    const position =
      (selectionContext.normalizedPosition + offset) % selectionContext.currentCycle.length;
    const selectedIndex = selectionContext.currentCycle[position];
    const candidateImage = selectionContext.images[selectedIndex];

    if (!candidateImage) {
      skippedInvalidImages.push({
        image: null,
        index: selectedIndex,
        cycle_position: position,
        reason: "image is missing",
        width: null,
        height: null,
        aspect_ratio: null,
      });
      continue;
    }

    const inspection = await inspectInstagramQueueImageCandidate(candidateImage);
    if (!inspection.ok) {
      const skippedImageLog = {
        queue_item_id: queueItemId,
        product_id: productId,
        previous_image: previousImage,
        next_candidate_image: candidateImage,
        skipped_image: candidateImage,
        skipped_reason: inspection.reason,
        image_width: inspection.metadata?.width ?? null,
        image_height: inspection.metadata?.height ?? null,
        aspect_ratio: formatImageAspectRatio(inspection.aspectRatio),
        cycle_position: position,
        image_index: selectedIndex,
      };
      console.log("[queue-image-rotation] skipped_invalid_image", skippedImageLog);
      skippedInvalidImages.push({
        image: candidateImage,
        index: selectedIndex,
        cycle_position: position,
        reason: inspection.reason,
        width: inspection.metadata?.width ?? null,
        height: inspection.metadata?.height ?? null,
        aspect_ratio: formatImageAspectRatio(inspection.aspectRatio),
      });
      continue;
    }

    const nextRotationState = buildQueueImageRotationStateForSelection(
      selectionContext.currentCycle,
      position,
      selectedIndex
    );
    console.log("[queue-image-rotation] selection", {
      queue_item_id: queueItemId,
      product_id: productId,
      previous_image: previousImage,
      next_candidate_image: firstCandidateImage,
      skipped_invalid_images: skippedInvalidImages,
      final_selected_image: candidateImage,
      updated_rotation_state: nextRotationState,
      image_rotation_source: selectionContext.source,
    });
    return {
      selectedImage: candidateImage,
      nextRotationState,
      imageCount: selectionContext.images.length,
      source: selectionContext.source,
      previousImage,
      nextCandidateImage: firstCandidateImage,
      skippedInvalidImages,
    };
  }

  console.log("[queue-image-rotation] selection", {
    queue_item_id: queueItemId,
    product_id: productId,
    previous_image: previousImage,
    next_candidate_image: firstCandidateImage,
    skipped_invalid_images: skippedInvalidImages,
    final_selected_image: null,
    updated_rotation_state: selectionContext.state,
    image_rotation_source: "no_valid_instagram_image",
  });
  return {
    selectedImage: null,
    nextRotationState: selectionContext.state,
    imageCount: selectionContext.images.length,
    source: "no_valid_instagram_image",
    previousImage,
    nextCandidateImage: firstCandidateImage,
    skippedInvalidImages,
  };
}

function serializeQueueItemForApi(item) {

  if (!item || typeof item !== "object") {

    return item;

  }

  const images = normalizeQueueImages(item.images);
  const storedLastImage = normalizeOptionalString(item.last_image);
  const currentPublishCount = Number.isInteger(item.publish_count)
    ? item.publish_count
    : 0;
  const isFreshPreview =
    currentPublishCount === 0 && Array.isArray(images) && images.length > 0;
  const previewImage = isFreshPreview
    ? images[0]
    : storedLastImage || (Array.isArray(images) && images.length > 0 ? images[0] : null);

  return {
    ...item,
    stored_last_image: storedLastImage,
    last_image: previewImage,
    preview_image: previewImage,
    preview_image_source: isFreshPreview
      ? "images[0]"
      : storedLastImage
        ? "last_image"
        : Array.isArray(images) && images.length > 0
          ? "images[0]_fallback"
          : "no_image_available",
  };

}

function normalizeQueueHashtags(value) {

  if (typeof value === "string") {

    const normalized = value.trim();
    return normalized ? normalized : null;

  }

  if (Array.isArray(value)) {

    const normalized = value
      .filter((item) => typeof item === "string" && item.trim())
      .map((item) => item.trim())
      .join(" ")
      .trim();

    return normalized ? normalized : null;

  }

  return null;

}

function resolveQueueIngestionOwnerUserId(req, requestedUserId) {

  const authenticatedUserId = normalizeOptionalPositiveIntegerString(req.user?.id);

  if (req.auth?.type === "jwt") {

    if (authenticatedUserId == null) {

      return { error: "Authenticated user context is missing" };

    }

    if (requestedUserId != null && requestedUserId !== authenticatedUserId) {

      return { error: "user_id does not match authenticated user" };

    }

    return {
      userId: authenticatedUserId,
      scope_source: requestedUserId != null ? "jwt_match" : "jwt",
    };

  }

  if (requestedUserId != null) {

    return { userId: requestedUserId, scope_source: "request_user_id" };

  }

  if (authenticatedUserId != null) {

    return { userId: authenticatedUserId, scope_source: "auth_context" };

  }

  return { error: "Missing queue user id" };

}

function buildLegacyQueueAddPayload(body) {

  const source = body || {};

  return {
    product_id: source.product_id,
    url: source.url,
    images: source.images ?? null,
    last_image: source.last_image || null,
    publish_count: Number.isInteger(source.publish_count) ? source.publish_count : 0,
    next_run_at: source.next_run_at || null,
    status: source.status || null,
    max_republish: source.max_republish,
    last_title: source.last_title || null,
    last_description: source.last_description || null,
    last_caption: source.last_caption || null,
    last_hashtags: source.last_hashtags || null,
  };

}

function pickFirstDefinedValue(...values) {

  for (const value of values) {

    if (value !== undefined) {

      return value;

    }

  }

  return undefined;

}

function sanitizeProductIdSegment(value) {

  const normalized = normalizeOptionalString(value)?.toLowerCase() || "";
  return normalized.replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");

}

function buildExternalUrlDerivedProductId(rawUrl) {

  const normalizedUrl = normalizeOptionalString(rawUrl);

  if (!normalizedUrl) {

    return null;

  }

  let parsedUrl = null;

  try {

    parsedUrl = new URL(normalizedUrl);

  } catch {

    return null;

  }

  const hostname = parsedUrl.hostname.toLowerCase();
  const pathname = parsedUrl.pathname || "/";

  if (hostname === "etsy.com" || hostname.endsWith(".etsy.com") || hostname === "www.etsy.com") {

    const listingId = pathname.match(/\/listing\/(\d+)/i)?.[1] || null;

    if (listingId) {

      return `etsy-${listingId}`;

    }

  }

  if (
    hostname === "amazon.com" ||
    hostname.endsWith(".amazon.com") ||
    hostname === "amzn.to" ||
    hostname.endsWith(".amzn.to")
  ) {

    const asin =
      pathname.match(/\/dp\/([A-Z0-9]{10})(?:[/?]|$)/i)?.[1] ||
      pathname.match(/\/gp\/product\/([A-Z0-9]{10})(?:[/?]|$)/i)?.[1] ||
      pathname.match(/\/gp\/aw\/d\/([A-Z0-9]{10})(?:[/?]|$)/i)?.[1] ||
      pathname.match(/\/gp\/offer-listing\/([A-Z0-9]{10})(?:[/?]|$)/i)?.[1] ||
      null;

    if (asin) {

      return `amazon-${asin.toUpperCase()}`;

    }

  }

  if (
    hostname === "aliexpress.com" ||
    hostname.endsWith(".aliexpress.com") ||
    hostname === "s.click.aliexpress.com" ||
    hostname.endsWith(".s.click.aliexpress.com")
  ) {

    const itemId =
      pathname.match(/\/item\/(\d+)\.html/i)?.[1] ||
      pathname.match(/\/i\/(\d+)\.html/i)?.[1] ||
      null;

    if (itemId) {

      return `aliexpress-${itemId}`;

    }

  }

  const domain = sanitizeProductIdSegment(
    hostname.replace(/^www\./, "").replace(/^m\./, "")
  ) || "external";
  const pathSegments = pathname.split("/").filter(Boolean);
  const lastSegment = pathSegments[pathSegments.length - 1] || "";
  const slug = sanitizeProductIdSegment(
    lastSegment.replace(/\.[a-z0-9]+$/i, "")
  ).slice(0, 40);

  if (slug) {

    return `external-${domain}-${slug}`;

  }

  const shortHash = crypto
    .createHash("sha1")
    .update(parsedUrl.toString())
    .digest("hex")
    .slice(0, 10);

  return `external-${domain}-${shortHash}`;

}

function validateAndNormalizeProductImportPayload(body) {

  const source = normalizeRequestBodyObject(body);
  const errors = [];
  const rawProductId = pickFirstDefinedValue(source.product_id, source.id);
  const rawUrl = pickFirstDefinedValue(source.url);
  const rawImages = pickFirstDefinedValue(source.images);
  const rawLastImage = pickFirstDefinedValue(
    source.last_image,
    source.selected_image,
    source.image,
    source.thumbnail
  );
  const rawLastTitle = pickFirstDefinedValue(
    source.last_title,
    source.title,
    source.name
  );
  const rawLastDescription = pickFirstDefinedValue(
    source.last_description,
    source.description,
    source.details
  );
  const url = normalizeOptionalString(rawUrl);
  const productId =
    normalizeOptionalString(rawProductId) || buildExternalUrlDerivedProductId(url);
  const imagesProvided =
    hasOwn(source, "images") || hasOwn(source, "image") || hasOwn(source, "thumbnail");
  const images = normalizeQueueImages(rawImages);
  const explicitLastImage = normalizeOptionalString(rawLastImage);
  const publishCount = hasOwn(source, "publish_count")
    ? normalizeOptionalInteger(source.publish_count)
    : 0;
  const nextRunAt = hasOwn(source, "next_run_at")
    ? normalizeOptionalTimestamp(source.next_run_at)
    : null;
  const status = hasOwn(source, "status")
    ? normalizeQueueItemStatus(source.status)
    : null;
  const maxRepublish = hasOwn(source, "max_republish")
    ? normalizeOptionalInteger(source.max_republish)
    : null;
  const lastTitle =
    hasOwn(source, "last_title") || hasOwn(source, "title") || hasOwn(source, "name")
      ? normalizeOptionalString(rawLastTitle)
      : null;
  const lastDescription =
    hasOwn(source, "last_description") ||
    hasOwn(source, "description") ||
    hasOwn(source, "details")
      ? normalizeOptionalString(rawLastDescription)
      : null;
  const lastCaption = hasOwn(source, "last_caption")
    ? normalizeOptionalString(source.last_caption)
    : null;
  const lastHashtags = hasOwn(source, "last_hashtags")
    ? normalizeQueueHashtags(source.last_hashtags)
    : normalizeQueueHashtags(source.hashtags);
  const workflowSource = hasOwn(source, "workflow_source")
    ? normalizeOptionalString(source.workflow_source)
    : null;
  const requestedUserId = hasOwn(source, "user_id")
    ? normalizeOptionalPositiveIntegerString(source.user_id)
    : null;

  if (!productId) {

    errors.push("product_id is required");

  }

  if (!url) {

    errors.push("url is required");

  }

  if (
    imagesProvided &&
    source.images != null &&
    source.images !== "" &&
    images == null
  ) {

    errors.push("images must be an array of image URLs, a single image URL, or valid JSON");

  }

  if (hasOwn(source, "publish_count") && (publishCount == null || publishCount < 0)) {

    errors.push("publish_count must be an integer >= 0");

  }

  if (hasOwn(source, "next_run_at") && nextRunAt == null) {

    errors.push("next_run_at must be a valid timestamp");

  }

  if (hasOwn(source, "max_republish") && (maxRepublish == null || maxRepublish < 0)) {

    errors.push("max_republish must be an integer >= 0");

  }

  if (hasOwn(source, "user_id") && requestedUserId == null) {

    errors.push("user_id must be a positive integer");

  }

  return {
    errors,
    value: {
      product_id: productId,
      url,
      images,
      last_image:
        Array.isArray(images) && images.length > 0
          ? null
          : explicitLastImage || null,
      publish_count: publishCount,
      next_run_at: nextRunAt,
      status,
      max_republish: maxRepublish,
      last_title: lastTitle,
      last_description: lastDescription,
      last_caption: lastCaption,
      last_hashtags: lastHashtags,
      workflow_source: workflowSource,
      requested_user_id: requestedUserId,
    },
  };

}

function describeImportPayloadValueType(value) {

  if (value === null) {

    return "null";

  }

  if (Array.isArray(value)) {

    return "array";

  }

  return typeof value;

}

function buildProtectedImportValidationErrorResponse(body, validationErrors) {

  const source = normalizeRequestBodyObject(body);
  const errors = {};
  const missingFields = [];
  const wrongTypes = {};
  const rawImages = pickFirstDefinedValue(source.images);
  const imagesArrayOfObjects =
    Array.isArray(rawImages) &&
    rawImages.length > 0 &&
    rawImages.every(
      (item) => item != null && typeof item === "object" && !Array.isArray(item)
    );

  for (const error of validationErrors) {

    if (error === "product_id is required") {

      errors.product_id = [error];
      missingFields.push("product_id");
      continue;

    }

    if (error === "url is required") {

      errors.url = [error];
      missingFields.push("url");
      continue;

    }

    if (error === "images must be an array of image URLs, a single image URL, or valid JSON") {

      errors.images = [error];
      wrongTypes.images = {
        expected: "array of strings, single string URL, or valid JSON",
        received: describeImportPayloadValueType(source.images),
      };
      continue;

    }

    if (error === "publish_count must be an integer >= 0") {

      errors.publish_count = [error];
      wrongTypes.publish_count = {
        expected: "integer >= 0",
        received: describeImportPayloadValueType(source.publish_count),
      };
      continue;

    }

    if (error === "next_run_at must be a valid timestamp") {

      errors.next_run_at = [error];
      wrongTypes.next_run_at = {
        expected: "valid timestamp",
        received: describeImportPayloadValueType(source.next_run_at),
      };
      continue;

    }

    if (error === "max_republish must be an integer >= 0") {

      errors.max_republish = [error];
      wrongTypes.max_republish = {
        expected: "integer >= 0",
        received: describeImportPayloadValueType(source.max_republish),
      };
      continue;

    }

    if (error === "user_id must be a positive integer") {

      errors.user_id = [error];
      wrongTypes.user_id = {
        expected: "positive integer",
        received: describeImportPayloadValueType(source.user_id),
      };
      continue;

    }

  }

  return {
    ok: false,
    error: "Invalid import payload",
    message: "Invalid import payload",
    errors,
    missing_fields: missingFields,
    wrong_types: wrongTypes,
    images_array_of_objects: imagesArrayOfObjects,
  };

}

async function createQueueItemFromPayload(userId, payload) {

  const resolvedPublishCount = Number.isInteger(payload?.publish_count)
    ? payload.publish_count
    : 0;
  const normalizedImages = normalizeQueueImages(payload?.images);
  const initialImageRotationState = buildInitialQueueImageRotationState(
    normalizedImages
  );
  const persistedLastImage =
    initialImageRotationState == null
      ? normalizeOptionalString(payload?.last_image)
      : null;

  const resolvedMaxRepublish = resolveQueueMaxRepublish(payload?.max_republish);

  console.log("[queue-debug] queue item creation payload", {
    user_id: userId,
    product_id: payload?.product_id,
    url: payload?.url,
    publish_count: payload?.publish_count,
    max_republish: payload?.max_republish,
    status: payload?.status,
    next_run_at: payload?.next_run_at,
    workflow_source: payload?.workflow_source ?? null,
  });

  console.log("[queue-debug] resolved max_republish default", {
    product_id: payload?.product_id,
    input_max_republish: payload?.max_republish,
    resolved_max_republish: resolvedMaxRepublish,
  });

  const insert = await pool.query(

    `

    INSERT INTO products_queue (

      user_id,

      product_id,

      url,

      images,

      last_image,

      publish_count,

      next_run_at,

      status,

      max_republish,

      last_title,

      last_description,

      last_caption,

      last_hashtags,

      image_rotation_state,

      created_at,

      updated_at

    ) VALUES (

      $1,$2,$3,$4::jsonb,$5,

      $6,

      COALESCE($7,NOW()),

      COALESCE($8,'scheduled'),

      $9,

      $10,$11,$12,$13,$14::jsonb,

      NOW(),NOW()

    )

    RETURNING *

    `,

    [

      userId,

      payload.product_id,

      payload.url,

      normalizedImages ? JSON.stringify(normalizedImages) : null,

      persistedLastImage,

      resolvedPublishCount,

      payload.next_run_at || null,

      payload.status || null,

      resolvedMaxRepublish,

      payload.last_title || null,

      payload.last_description || null,

      payload.last_caption || null,

      payload.last_hashtags || null,

      initialImageRotationState
        ? JSON.stringify(initialImageRotationState)
        : null,

    ]

  );

  return insert.rows[0];

}

function generateWebhookEndpointKey() {

  return crypto.randomBytes(18).toString("hex");

}

function generateWebhookSigningSecret() {

  return crypto.randomBytes(24).toString("hex");

}

function getWebhookSignatureHeader(req) {

  return normalizeOptionalString(
    req.headers["x-qilano-signature"] || req.headers["x-webhook-signature"]
  );

}

function getWebhookSecretHeader(req) {

  return normalizeOptionalString(req.headers["x-webhook-secret"]);

}

function buildWebhookSignaturePayload(body) {

  return JSON.stringify(normalizeRequestBodyObject(body));

}

function safeCompareStrings(a, b) {

  if (typeof a !== "string" || typeof b !== "string") {

    return false;

  }

  const left = Buffer.from(a);
  const right = Buffer.from(b);

  if (left.length !== right.length) {

    return false;

  }

  return crypto.timingSafeEqual(left, right);

}

function verifyWebhookRequestSignature(req, endpoint) {

  const secretHeader = getWebhookSecretHeader(req);
  const signatureHeader = getWebhookSignatureHeader(req);
  const secret = normalizeOptionalString(endpoint?.signing_secret);

  if (!secret) {

    return { valid: true, mode: "none" };

  }

  if (secretHeader) {

    return {
      valid: safeCompareStrings(secretHeader, secret),
      mode: "shared_secret",
    };

  }

  if (signatureHeader) {

    const payload = buildWebhookSignaturePayload(req.body);
    const expectedDigest = crypto
      .createHmac("sha256", secret)
      .update(payload)
      .digest("hex");
    const normalizedHeader = signatureHeader.replace(/^sha256=/i, "");

    return {
      valid: safeCompareStrings(normalizedHeader, expectedDigest),
      mode: "hmac_sha256",
    };

  }

  return { valid: true, mode: "unsigned" };

}

function buildWebhookPublicBaseUrl(req) {

  const configuredBaseUrl = normalizeOptionalString(
    process.env.WEBHOOK_PUBLIC_BASE_URL ||
      process.env.PUBLIC_BASE_URL ||
      process.env.APP_BASE_URL
  );

  if (configuredBaseUrl) {

    return configuredBaseUrl.replace(/\/+$/, "");

  }

  const protocol = normalizeOptionalString(req.headers["x-forwarded-proto"]) || req.protocol || "http";
  const host = normalizeOptionalString(req.headers["x-forwarded-host"]) || req.get("host");

  return host ? `${protocol}://${host}` : `http://127.0.0.1:${PORT}`;

}

function buildInboundWebhookUrl(req, endpointKey) {

  return `${buildWebhookPublicBaseUrl(req)}/webhooks/inbound/${endpointKey}`;

}

function sanitizeWebhookEndpoint(row, req) {

  if (!row) {

    return null;

  }

  return {
    id: row.id,
    name: row.name,
    event: row.event,
    endpoint_key: row.endpoint_key,
    active: row.active,
    source_label: row.source_label,
    user_id: row.user_id,
    created_at: row.created_at,
    updated_at: row.updated_at,
    has_signing_secret: Boolean(row.signing_secret),
    webhook_url: buildInboundWebhookUrl(req, row.endpoint_key),
  };

}

function validateAndNormalizeWebhookEndpointPayload(body) {

  const source = normalizeRequestBodyObject(body);
  const errors = [];
  const requestedUserId = hasOwn(source, "user_id")
    ? normalizeOptionalPositiveIntegerString(source.user_id)
    : null;
  const name = normalizeOptionalString(source.name);
  const event = hasOwn(source, "event")
    ? normalizeOptionalString(source.event)
    : DEFAULT_WEBHOOK_EVENT;
  const sourceLabel = hasOwn(source, "source_label")
    ? normalizeOptionalString(source.source_label)
    : null;
  const active = hasOwn(source, "active")
    ? normalizeOptionalBoolean(source.active)
    : true;

  if (!name) {

    errors.push("name is required");

  }

  if (!event) {

    errors.push("event is required");

  } else if (event !== DEFAULT_WEBHOOK_EVENT) {

    errors.push(`event must be ${DEFAULT_WEBHOOK_EVENT}`);

  }

  if (hasOwn(source, "active") && active == null) {

    errors.push("active must be a boolean");

  }

  if (hasOwn(source, "user_id") && requestedUserId == null) {

    errors.push("user_id must be a positive integer");

  }

  return {
    errors,
    value: {
      name,
      event,
      source_label: sourceLabel,
      active,
      requested_user_id: requestedUserId,
    },
  };

}

function buildWebhookDeliverySummary(payload) {

  const normalized = validateAndNormalizeProductImportPayload(payload);
  const value = normalized.value || {};

  return {
    product_id: value.product_id || null,
    url: value.url || null,
    title: value.last_title || null,
    image:
      (Array.isArray(value.images) && value.images.length > 0
        ? value.images[0]
        : value.last_image) || null,
    image_count: Array.isArray(value.images) ? value.images.length : 0,
    status: value.status || null,
    workflow_source: value.workflow_source || null,
    errors: normalized.errors,
  };

}

function extractWebhookImportPayload(body) {

  const source = normalizeRequestBodyObject(body);
  const event = normalizeOptionalString(source.event) || DEFAULT_WEBHOOK_EVENT;
  const data = source.data && typeof source.data === "object" && !Array.isArray(source.data)
    ? source.data
    : null;
  const product = source.product && typeof source.product === "object" && !Array.isArray(source.product)
    ? source.product
    : null;
  const candidate =
    product ||
    (data?.product && typeof data.product === "object" && !Array.isArray(data.product)
      ? data.product
      : null) ||
    data ||
    source;

  return {
    event,
    payload: normalizeRequestBodyObject(candidate),
  };

}

async function createWebhookEndpoint(req, ownerUserId, payload) {

  const endpointKey = generateWebhookEndpointKey();
  const signingSecret = generateWebhookSigningSecret();
  const created = await pool.query(
    `
    INSERT INTO inbound_webhook_endpoints (
      user_id,
      name,
      event,
      endpoint_key,
      signing_secret,
      active,
      source_label,
      created_at,
      updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,NOW(),NOW()
    )
    RETURNING *
    `,
    [
      ownerUserId,
      payload.name,
      payload.event,
      endpointKey,
      signingSecret,
      payload.active !== false,
      payload.source_label || null,
    ]
  );

  return {
    endpoint: created.rows[0],
    signing_secret: signingSecret,
    webhook_url: buildInboundWebhookUrl(req, endpointKey),
  };

}

async function createWebhookDelivery(endpointId, event, requestBody) {

  const extracted = extractWebhookImportPayload(requestBody);
  const payloadJson = normalizeOptionalJson(normalizeRequestBodyObject(requestBody));
  const payloadSummaryJson = normalizeOptionalJson(buildWebhookDeliverySummary(extracted.payload));
  const inserted = await pool.query(
    `
    INSERT INTO inbound_webhook_deliveries (
      endpoint_id,
      event,
      received_at,
      status,
      payload,
      payload_summary,
      created_at,
      updated_at
    ) VALUES (
      $1,$2,NOW(),'received',$3::jsonb,$4::jsonb,NOW(),NOW()
    )
    RETURNING *
    `,
    [endpointId, event, payloadJson, payloadSummaryJson]
  );

  return inserted.rows[0];

}

async function finalizeWebhookDelivery(deliveryId, updates) {

  const result = await pool.query(
    `
    UPDATE inbound_webhook_deliveries
    SET status = COALESCE($2, status),
        queue_item_id = COALESCE($3, queue_item_id),
        error_message = COALESCE($4, error_message),
        updated_at = NOW()
    WHERE id = $1
    RETURNING *
    `,
    [
      deliveryId,
      updates.status || null,
      updates.queue_item_id || null,
      updates.error_message || null,
    ]
  );

  return result.rows[0];

}

function sanitizeSocialAccount(row) {

  if (!row) {

    return null;

  }

  return {
    id: row.id,
    platform: row.platform,
    handle: row.handle,
    status: row.status,
    hidden: row.hidden,
    is_default: row.is_default,
    token_expires_at: row.token_expires_at,
    daily_limit: row.daily_limit,
    daily_published: row.daily_published,
    last_used_at: row.last_used_at,
    instagram_business_account_id: row.instagram_business_account_id,
    facebook_page_id: row.facebook_page_id,
    instagram_user_id: row.instagram_user_id,
    created_at: row.created_at,
    updated_at: row.updated_at,
    has_access_token: Boolean(row.has_access_token),
    has_refresh_token: Boolean(row.has_refresh_token),
    has_raw: Boolean(row.has_raw),
  };

}

function validateAndNormalizeSocialAccountPayload(body) {

  const source = body || {};
  const errors = [];
  const platform = normalizeRequiredPlatform(source.platform);
  const handle = normalizeOptionalString(source.handle);
  const status = hasOwn(source, "status")
    ? normalizeOptionalString(source.status)
    : null;
  const hidden = hasOwn(source, "hidden")
    ? normalizeOptionalBoolean(source.hidden)
    : null;
  const isDefault = hasOwn(source, "is_default")
    ? normalizeOptionalBoolean(source.is_default)
    : null;
  const accessToken = hasOwn(source, "access_token")
    ? normalizeOptionalString(source.access_token)
    : null;
  const refreshToken = hasOwn(source, "refresh_token")
    ? normalizeOptionalString(source.refresh_token)
    : null;
  const tokenExpiresAt = hasOwn(source, "token_expires_at")
    ? normalizeOptionalTimestamp(source.token_expires_at)
    : null;
  const dailyLimit = hasOwn(source, "daily_limit")
    ? normalizeOptionalInteger(source.daily_limit)
    : null;
  const instagramBusinessAccountId = hasOwn(
    source,
    "instagram_business_account_id"
  )
    ? normalizeOptionalString(source.instagram_business_account_id)
    : null;
  const facebookPageId = hasOwn(source, "facebook_page_id")
    ? normalizeOptionalString(source.facebook_page_id)
    : null;
  const instagramUserId = hasOwn(source, "instagram_user_id")
    ? normalizeOptionalString(source.instagram_user_id)
    : null;
  const raw = hasOwn(source, "raw") ? normalizeOptionalJson(source.raw) : null;

  if (!platform) {

    errors.push("platform is required");

  }

  if (hasOwn(source, "hidden") && hidden == null) {

    errors.push("hidden must be a boolean");

  }

  if (hasOwn(source, "is_default") && isDefault == null) {

    errors.push("is_default must be a boolean");

  }

  if (hasOwn(source, "daily_limit")) {

    if (dailyLimit == null || dailyLimit < 0) {

      errors.push("daily_limit must be an integer >= 0");

    }

  }

  if (hasOwn(source, "token_expires_at") && tokenExpiresAt == null) {

    errors.push("token_expires_at must be a valid date");

  }

  if (hasOwn(source, "raw") && source.raw != null && raw == null) {

    errors.push("raw must be valid JSON or a serializable value");

  }

  const hasInstagramIdentity =
    Boolean(instagramBusinessAccountId) ||
    Boolean(facebookPageId) ||
    Boolean(handle);

  if (platform === "instagram" && !hasInstagramIdentity) {

    errors.push(
      "instagram account requires instagram_business_account_id, facebook_page_id, or handle"
    );

  }

  if (platform !== "instagram" && !handle) {

    errors.push("handle is required when platform is not instagram");

  }

  return {
    errors,
    value: {
      platform,
      handle,
      status,
      hidden,
      is_default: isDefault,
      access_token: accessToken,
      refresh_token: refreshToken,
      token_expires_at: tokenExpiresAt,
      daily_limit: dailyLimit,
      instagram_business_account_id: instagramBusinessAccountId,
      facebook_page_id: facebookPageId,
      instagram_user_id: instagramUserId,
      raw,
    },
  };

}

function validateAndNormalizeSocialAccountsAddRequest(body) {

  const source = body || {};
  const hasAccountsArray = hasOwn(source, "accounts");

  if (!hasAccountsArray) {

    const single = validateAndNormalizeSocialAccountPayload(source);
    return {
      errors: single.errors,
      items: single.errors.length > 0 ? [] : [{ body: source, value: single.value }],
      isBulk: false,
    };

  }

  const errors = [];
  const platform = normalizeRequiredPlatform(source.platform);

  if (!platform) {

    errors.push("platform is required");

  }

  if (!Array.isArray(source.accounts)) {

    errors.push("accounts must be an array");
    return { errors, items: [], isBulk: true };

  }

  if (source.accounts.length === 0) {

    errors.push("accounts must contain at least one account");
    return { errors, items: [], isBulk: true };

  }

  const items = [];

  source.accounts.forEach((account, index) => {

    if (!account || typeof account !== "object" || Array.isArray(account)) {

      errors.push(`accounts[${index}] must be an object`);
      return;

    }

    const mergedAccount = {
      ...account,
      platform,
    };
    const result = validateAndNormalizeSocialAccountPayload(mergedAccount);

    if (result.errors.length > 0) {

      for (const error of result.errors) {

        if (error === "platform is required") {
          continue;
        }

        errors.push(`accounts[${index}].${error}`);

      }
      return;

    }

    items.push({
      body: account,
      value: result.value,
    });

  });

  return {
    errors,
    items,
    isBulk: true,
  };

}

function resolveSocialAccountIdentity(payload) {

  if (payload.platform === "instagram" && payload.instagram_business_account_id) {

    return {
      key: "instagram_business_account_id",
      description:
        "platform + instagram_business_account_id (highest priority)",
      where: "platform = $1 AND instagram_business_account_id = $2",
      values: [payload.platform, payload.instagram_business_account_id],
    };

  }

  if (payload.platform === "instagram" && payload.facebook_page_id) {

    return {
      key: "facebook_page_id",
      description:
        "platform + facebook_page_id (fallback when instagram_business_account_id is missing)",
      where: "platform = $1 AND facebook_page_id = $2",
      values: [payload.platform, payload.facebook_page_id],
    };

  }

  return {
    key: "handle",
    description:
      "platform + lower(handle) (fallback when account ids are missing)",
    where: "platform = $1 AND lower(handle) = lower($2)",
    values: [payload.platform, payload.handle],
  };

}

function buildSocialAccountMutations(body, payload) {

  const insertColumns = ["platform"];
  const insertValues = [];
  const updateAssignments = [];
  const updateValues = [];

  const fieldConfigs = [
    { key: "handle", cast: null, defaultValue: null },
    { key: "status", cast: null, defaultValue: "active" },
    { key: "hidden", cast: null, defaultValue: false },
    { key: "is_default", cast: null, defaultValue: false },
    { key: "access_token", cast: null, defaultValue: null },
    { key: "refresh_token", cast: null, defaultValue: null },
    { key: "token_expires_at", cast: null, defaultValue: null },
    { key: "daily_limit", cast: null, defaultValue: 0 },
    { key: "instagram_business_account_id", cast: null, defaultValue: null },
    { key: "facebook_page_id", cast: null, defaultValue: null },
    { key: "instagram_user_id", cast: null, defaultValue: null },
    { key: "raw", cast: "::jsonb", defaultValue: null },
  ];

  for (const field of fieldConfigs) {

    const provided = hasOwn(body, field.key);
    const value = provided ? payload[field.key] : field.defaultValue;
    insertColumns.push(field.key);
    insertValues.push({ value, cast: field.cast });

    if (provided) {

      updateAssignments.push({ key: field.key, cast: field.cast });
      updateValues.push(value);

    }

  }

  return {
    insertColumns,
    insertValues,
    updateAssignments,
    updateValues,
  };

}

async function ensureSocialAccountsTable() {

  const sql = await fs.readFile(
    path.join(__dirname, "sql", "social_accounts.sql"),
    "utf8"
  );
  await pool.query(sql);

}

async function ensureInboundWebhookTables() {

  const sql = await fs.readFile(
    path.join(__dirname, "sql", "inbound_webhooks.sql"),
    "utf8"
  );
  await pool.query(sql);

}

async function ensureProductsQueueImageRotationState() {

  await pool.query(`
    ALTER TABLE products_queue
    ADD COLUMN IF NOT EXISTS image_rotation_state JSONB
  `);

}

async function ensureRuntimeSettingsTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${INTERNAL_REPOST_SETTINGS_TABLE} (
      setting_key TEXT PRIMARY KEY,
      value_json JSONB,
      is_secret BOOLEAN NOT NULL DEFAULT false,
      updated_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function loadRuntimeSettingsFromDb() {
  const result = await pool.query(
    `
    SELECT setting_key, value_json, is_secret, updated_by, updated_at
    FROM ${INTERNAL_REPOST_SETTINGS_TABLE}
    WHERE setting_key LIKE $1 OR setting_key LIKE $2
    ORDER BY setting_key ASC
    `,
    [`${INTERNAL_REPOST_SETTINGS_PREFIX}%`, `${OPENROUTER_SETTINGS_PREFIX}%`]
  );

  const nextValues = new Map();
  for (const row of result.rows) {
    nextValues.set(row.setting_key, {
      value: row.value_json,
      isSecret: row.is_secret === true,
      updatedBy: row.updated_by || null,
      updatedAt: row.updated_at || null,
    });
  }

  runtimeSettingsState.values = nextValues;
  runtimeSettingsState.loaded = true;
  runtimeSettingsState.lastLoadedAt = new Date().toISOString();
  return nextValues;
}

async function upsertRuntimeSetting({
  settingKey,
  value,
  isSecret = false,
  updatedBy = null,
}) {
  await pool.query(
    `
    INSERT INTO ${INTERNAL_REPOST_SETTINGS_TABLE} (
      setting_key,
      value_json,
      is_secret,
      updated_by,
      created_at,
      updated_at
    ) VALUES (
      $1,
      $2::jsonb,
      $3,
      $4,
      NOW(),
      NOW()
    )
    ON CONFLICT (setting_key)
    DO UPDATE SET
      value_json = EXCLUDED.value_json,
      is_secret = EXCLUDED.is_secret,
      updated_by = EXCLUDED.updated_by,
      updated_at = NOW()
    `,
    [settingKey, JSON.stringify(value), isSecret === true, updatedBy]
  );
}

function buildInternalRepostActivityEntry(level, event, context = {}) {
  const timestamp = new Date().toISOString();
  const status = normalizeOptionalString(context.status) || null;
  const message =
    normalizeOptionalString(context.message) ||
    normalizeOptionalString(context.note) ||
    null;
  const isTerminalEvent =
    Boolean(status) &&
    ["done", "failed", "disabled", "published", "publish_failed", "error", "no_item"].includes(
      status
    );

  return {
    level,
    event,
    queue_item_id: Number.isInteger(context.queue_item_id) ? context.queue_item_id : null,
    trace_id: normalizeOptionalString(context.trace_id || context.traceId) || null,
    action: normalizeOptionalString(context.action) || null,
    status,
    message,
    platform: normalizeOptionalString(context.platform) || "instagram",
    started_at: timestamp,
    finished_at: isTerminalEvent ? timestamp : null,
    error_summary:
      level === "error" || status === "error" || status === "publish_failed" || status === "failed"
        ? message
        : null,
  };
}

function recordInternalRepostActivity(level, event, context = {}) {
  internalRepostActivity.unshift(buildInternalRepostActivityEntry(level, event, context));
  if (internalRepostActivity.length > INTERNAL_REPOST_ACTIVITY_LIMIT) {
    internalRepostActivity.length = INTERNAL_REPOST_ACTIVITY_LIMIT;
  }
}



// ---- Auth Middleware ----



function auth(req, res, next) {

  const { scheme, token } = parseAuthorizationHeader(req);



  if (scheme !== "Bearer" || !token) {

    return res

      .status(401)

      .json({ error: "Missing or invalid Authorization header" });

  }



  try {

    const payload = jwt.verify(token, JWT_SECRET);

    req.user = { id: payload.id, email: payload.email };

    return next();

  } catch (e) {

    return res.status(401).json({ error: "Invalid or expired token" });

  }

}

function queueAuth(req, res, next) {

  const { exists, scheme, token, tokenLength } = parseAuthorizationHeader(req);
  const expectedTokenSources = ["JWT_SECRET"];
  const hasStaticApiToken = Boolean(QILANO_API_TOKEN);

  if (hasStaticApiToken) {
    expectedTokenSources.unshift("QILANO_API_TOKEN");
  }

  logQueueAuth({
    path: req.originalUrl || req.url,
    method: req.method,
    authorizationHeaderExists: exists,
    authSchemeUsed: scheme || null,
    tokenLength,
    expectedTokenSourceNames: expectedTokenSources,
  });

  if (scheme !== "Bearer" || !token) {
    logQueueAuth({
      path: req.originalUrl || req.url,
      comparisonFailed: true,
      tokenConsideredExpired: false,
      reason: "missing_or_invalid_authorization_header",
    });

    return res
      .status(401)
      .json({ error: "Missing or invalid Authorization header" });
  }

  if (hasStaticApiToken) {
    const staticTokenMatched = token === QILANO_API_TOKEN;

    logQueueAuth({
      path: req.originalUrl || req.url,
      expectedTokenSourceName: "QILANO_API_TOKEN",
      comparisonFailed: !staticTokenMatched,
      tokenConsideredExpired: false,
    });

    if (staticTokenMatched) {
      const userId = resolveQueueUserId(req);

      if (userId == null) {
        logQueueAuth({
          path: req.originalUrl || req.url,
          expectedTokenSourceName: "x-qilano-user-id|x-user-id|user_id",
          comparisonFailed: true,
          tokenConsideredExpired: false,
          reason: "missing_queue_user_id_for_static_token",
        });

        return res.status(401).json({
          error: "Missing queue user id",
          message:
            "Send X-QILANO-USER-ID (or user_id) when using Authorization: Bearer <QILANO_API_TOKEN>",
        });
      }

      req.user = { id: userId };
      req.auth = { type: "api_token", source: "QILANO_API_TOKEN" };
      return next();
    }
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET);

    req.user = { id: payload.id, email: payload.email };
    req.auth = { type: "jwt", source: "JWT_SECRET" };

    logQueueAuth({
      path: req.originalUrl || req.url,
      expectedTokenSourceName: "JWT_SECRET",
      comparisonFailed: false,
      tokenConsideredExpired: false,
    });

    return next();
  } catch (e) {
    const tokenConsideredExpired = e?.name === "TokenExpiredError";

    logQueueAuth({
      path: req.originalUrl || req.url,
      expectedTokenSourceName: "JWT_SECRET",
      comparisonFailed: true,
      tokenConsideredExpired,
      jwtErrorName: e?.name || null,
    });

    return res.status(401).json({ error: "Invalid or expired token" });
  }

}

function socialAccountsAuth(req, res, next) {

  const { scheme, token } = parseAuthorizationHeader(req);



  if (scheme !== "Bearer" || !token) {

    return res.status(401).json({
      error: "Missing Authorization header",
      message: "Send Authorization: Bearer <QILANO_API_TOKEN>",
    });

  }



  if (!QILANO_API_TOKEN || token !== QILANO_API_TOKEN) {

    return res.status(401).json({
      error: "Invalid API token",
      message: "Authorization bearer token is invalid for social-accounts routes",
    });

  }



  return next();

}



// ========================

// Health

// ========================



app.get("/health", async (req, res) => {

  try {

    const r = await pool.query("SELECT 1 AS ok");

    res.json({ status: "ok", db: r.rows?.[0]?.ok === 1 ? "ok" : "unknown" });

  } catch (e) {

    res.status(500).json({ status: "error", db: "failed", message: e.message });

  }

});



// ========================

// Auth: Register

// ========================



app.post("/auth/register", async (req, res) => {

  try {

    const { email, password } = req.body || {};

    if (!email || !password) {

      return res.status(400).json({ error: "email and password are required" });

    }



    const exists = await pool.query("SELECT id FROM users WHERE email=$1", [

      email,

    ]);



    if (exists.rowCount > 0) {

      return res.status(409).json({ error: "Email already exists" });

    }



    const hashed = await bcrypt.hash(password, 10);



    const created = await pool.query(

      "INSERT INTO users(email, password) VALUES($1, $2) RETURNING id, email, created_at",

      [email, hashed]

    );



    const user = safeUser(created.rows[0]);

    const token = signToken(user);



    return res.json({ user, token });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});



// ========================

// Auth: Login

// ========================



app.post("/auth/login", async (req, res) => {

  try {

    const { email, password } = req.body || {};

    if (!email || !password) {

      return res.status(400).json({ error: "email and password are required" });

    }



    const r = await pool.query(

      "SELECT id, email, password, created_at FROM users WHERE email=$1",

      [email]

    );



    if (r.rowCount === 0) {

      return res.status(401).json({ error: "Invalid credentials" });

    }



    const row = r.rows[0];

    const ok = await bcrypt.compare(password, row.password);



    if (!ok) {

      return res.status(401).json({ error: "Invalid credentials" });

    }



    const user = safeUser(row);

    const token = signToken(user);



    return res.json({ user, token });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});



// ========================

// Protected: /me

// ========================



app.get("/me", auth, (req, res) => {

  return res.json({ user: req.user });

});

// ========================

// Protected: Social Accounts Add

// ========================

app.post("/social-accounts/add", socialAccountsAuth, async (req, res) => {

  const { errors, items, isBulk } = validateAndNormalizeSocialAccountsAddRequest(
    req.body
  );

  if (errors.length > 0) {

    return res.status(400).json({ error: "Validation failed", details: errors });

  }
  const client = await pool.connect();

  try {

    await client.query("BEGIN");
    const responseItems = [];
    const upsertRules = [];

    for (const item of items) {

      const { body, value } = item;
      const identity = resolveSocialAccountIdentity(value);
      const mutations = buildSocialAccountMutations(body || {}, value);
      const existing = await client.query(
        `
        SELECT id, platform
        FROM social_accounts
        WHERE ${identity.where}
        FOR UPDATE
        LIMIT 1
        `,
        identity.values
      );

      let accountId = existing.rows[0]?.id || null;

      if (existing.rowCount > 0) {

        if (mutations.updateAssignments.length > 0) {

          const setClauses = mutations.updateAssignments.map(
            (field, index) =>
              `${field.key} = $${index + 2}${field.cast || ""}`
          );
          setClauses.push("updated_at = NOW()");
          const updateResult = await client.query(
            `
            UPDATE social_accounts
            SET ${setClauses.join(", ")}
            WHERE id = $1
            RETURNING ${SOCIAL_ACCOUNT_SELECT_FIELDS}
            `,
            [accountId, ...mutations.updateValues]
          );
          accountId = updateResult.rows[0].id;

        }

      } else {

        const placeholders = mutations.insertValues.map(
          (field, index) => `$${index + 2}${field.cast || ""}`
        );
        const insertResult = await client.query(
          `
          INSERT INTO social_accounts (
            ${mutations.insertColumns.join(", ")},
            created_at,
            updated_at
          )
          VALUES (
            $1,
            ${placeholders.join(", ")},
            NOW(),
            NOW()
          )
          RETURNING ${SOCIAL_ACCOUNT_SELECT_FIELDS}
          `,
          [value.platform, ...mutations.insertValues.map((field) => field.value)]
        );
        accountId = insertResult.rows[0].id;

      }

      if (value.is_default === true) {

        await client.query(
          `
          UPDATE social_accounts
          SET is_default = CASE WHEN id = $2 THEN true ELSE false END,
              updated_at = NOW()
          WHERE platform = $1
            AND (is_default = true OR id = $2)
          `,
          [value.platform, accountId]
        );

      }

      const account = await client.query(
        `
        SELECT ${SOCIAL_ACCOUNT_SELECT_FIELDS}
        FROM social_accounts
        WHERE id = $1
        `,
        [accountId]
      );
      responseItems.push(sanitizeSocialAccount(account.rows[0]));
      upsertRules.push(identity.description);

    }

    await client.query("COMMIT");

    if (!isBulk) {

      return res.json({
        item: responseItems[0],
        upsert_rule: upsertRules[0],
      });

    }

    return res.json({
      items: responseItems,
      upsert_rules: upsertRules,
    });

  } catch (e) {

    await client.query("ROLLBACK");

    if (e?.code === "23505") {

      return res.status(409).json({
        error: "Social account already exists for this identity",
        message: e.detail || e.message,
      });

    }

    return res.status(500).json({ error: "Server error", message: e.message });

  } finally {

    client.release();

  }

});

// ========================

// Protected: Social Accounts List

// ========================

app.get("/social-accounts/list", socialAccountsAuth, async (req, res) => {

  try {

    const platform = normalizeRequiredPlatform(req.query.platform);
    const values = [];
    let query = `
      SELECT ${SOCIAL_ACCOUNT_SELECT_FIELDS}
      FROM social_accounts
      WHERE 1 = 1
    `;

    if (platform) {

      values.push(platform);
      query += ` AND platform = $${values.length}`;

    }

    query += `
      ORDER BY
        is_default DESC,
        CASE WHEN status = 'active' THEN 0 ELSE 1 END,
        updated_at DESC,
        id DESC
    `;

    const accounts = await pool.query(query, values);

    return res.json({
      total: accounts.rowCount,
      items: accounts.rows.map(sanitizeSocialAccount),
    });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

// ========================

// Protected: Social Accounts Toggle

// ========================

app.post("/social-accounts/toggle", socialAccountsAuth, async (req, res) => {

  const id = Number(req.body?.id);
  const hidden = hasOwn(req.body || {}, "hidden")
    ? normalizeOptionalBoolean(req.body.hidden)
    : null;
  const isDefault = hasOwn(req.body || {}, "is_default")
    ? normalizeOptionalBoolean(req.body.is_default)
    : null;
  const active = hasOwn(req.body || {}, "active")
    ? normalizeOptionalBoolean(req.body.active)
    : null;
  const status = hasOwn(req.body || {}, "status")
    ? normalizeOptionalString(req.body.status)
    : null;

  if (!Number.isFinite(id)) {

    return res.status(400).json({ error: "id must be a valid number" });

  }

  if (hasOwn(req.body || {}, "hidden") && hidden == null) {

    return res.status(400).json({ error: "hidden must be a boolean" });

  }

  if (hasOwn(req.body || {}, "is_default") && isDefault == null) {

    return res.status(400).json({ error: "is_default must be a boolean" });

  }

  if (hasOwn(req.body || {}, "active") && active == null) {

    return res.status(400).json({ error: "active must be a boolean" });

  }

  const nextStatus = status || (active === true ? "active" : active === false ? "inactive" : null);
  const assignments = [];
  const values = [id];

  if (nextStatus) {

    values.push(nextStatus);
    assignments.push(`status = $${values.length}`);

  }

  if (hidden != null) {

    values.push(hidden);
    assignments.push(`hidden = $${values.length}`);

  }

  if (isDefault != null) {

    values.push(isDefault);
    assignments.push(`is_default = $${values.length}`);

  }

  if (assignments.length === 0) {

    return res.status(400).json({
      error: "Provide at least one of status, active, hidden, or is_default",
    });

  }

  const client = await pool.connect();

  try {

    await client.query("BEGIN");

    const updated = await client.query(
      `
      UPDATE social_accounts
      SET ${assignments.join(", ")},
          updated_at = NOW()
      WHERE id = $1
      RETURNING ${SOCIAL_ACCOUNT_SELECT_FIELDS}
      `,
      values
    );

    if (updated.rowCount === 0) {

      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Social account not found" });

    }

    const account = updated.rows[0];

    if (isDefault === true) {

      await client.query(
        `
        UPDATE social_accounts
        SET is_default = CASE WHEN id = $2 THEN true ELSE false END,
            updated_at = NOW()
        WHERE platform = $1
          AND (is_default = true OR id = $2)
        `,
        [account.platform, account.id]
      );

    }

    if (isDefault === false) {

      await client.query(
        `
        UPDATE social_accounts
        SET is_default = false,
            updated_at = NOW()
        WHERE id = $1
        `,
        [account.id]
      );

    }

    const refreshed = await client.query(
      `
      SELECT ${SOCIAL_ACCOUNT_SELECT_FIELDS}
      FROM social_accounts
      WHERE id = $1
      `,
      [account.id]
    );

    await client.query("COMMIT");

    return res.json({ item: sanitizeSocialAccount(refreshed.rows[0]) });

  } catch (e) {

    await client.query("ROLLBACK");

    if (e?.code === "23505") {

      return res.status(409).json({
        error: "Another default account already exists for this platform",
        message: e.detail || e.message,
      });

    }

    return res.status(500).json({ error: "Server error", message: e.message });

  } finally {

    client.release();

  }

});

// ========================

// Protected: Social Accounts Update Limit

// ========================

app.post("/social-accounts/update-limit", socialAccountsAuth, async (req, res) => {

  try {

    const id = Number(req.body?.id);
    const dailyLimit = normalizeOptionalInteger(req.body?.daily_limit);

    if (!Number.isFinite(id)) {

      return res.status(400).json({ error: "id must be a valid number" });

    }

    if (dailyLimit == null || dailyLimit < 0) {

      return res.status(400).json({ error: "daily_limit must be an integer >= 0" });

    }

    const updated = await pool.query(
      `
      UPDATE social_accounts
      SET daily_limit = $2,
          updated_at = NOW()
      WHERE id = $1
      RETURNING ${SOCIAL_ACCOUNT_SELECT_FIELDS}
      `,
      [id, dailyLimit]
    );

    if (updated.rowCount === 0) {

      return res.status(404).json({ error: "Social account not found" });

    }

    return res.json({ item: sanitizeSocialAccount(updated.rows[0]) });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});



// ========================

// Protected: Queue Add

// ========================



app.post("/queue/add", queueAuth, async (req, res) => {

  try {

    const userId = req.user.id;
    const payload = buildLegacyQueueAddPayload(req.body);

    if (!payload.product_id || !payload.url) {

      return res.status(400).json({ error: "product_id and url are required" });

    }

    const item = await createQueueItemFromPayload(userId, payload);

    return res.json({ item: serializeQueueItemForApi(item) });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

async function handleProtectedProductImportRequest(req, res) {

  try {

    const normalized = validateAndNormalizeProductImportPayload(req.body);

    if (normalized.errors.length > 0) {

      return res
        .status(400)
        .json(buildProtectedImportValidationErrorResponse(req.body, normalized.errors));

    }

    const owner = resolveQueueIngestionOwnerUserId(
      req,
      normalized.value.requested_user_id
    );

    if (owner.error) {

      return res.status(400).json({ error: owner.error });

    }

    const item = await createQueueItemFromPayload(owner.userId, normalized.value);

    return res.json({
      item: serializeQueueItemForApi(item),
      import: {
        owner_user_id: owner.userId,
        owner_scope_source: owner.scope_source,
        workflow_source: normalized.value.workflow_source,
      },
    });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

}

app.post("/imports/products", queueAuth, async (req, res) => {

  return handleProtectedProductImportRequest(req, res);

});

app.post("/internal/import-products", queueAuth, async (req, res) => {

  return handleProtectedProductImportRequest(req, res);

});

function buildExternalUrlImportFailure(status, code, message, provider = null, details = null) {

  const payload = {
    ok: false,
    status,
    code,
    message,
    provider: provider || null,
  };

  if (details != null) {

    payload.details = details;

  }

  return payload;

}

function sendExternalUrlImportFailure(res, status, code, message, provider = null, details = null) {

  return res.status(status).json(
    buildExternalUrlImportFailure(status, code, message, provider, details)
  );

}

function mapExternalUrlExtractionReasonToCode(reason) {

  if (reason === "captcha") {

    return "captcha_blocked";

  }

  if (reason === "blocked" || reason === "forbidden" || reason === "rate_limited") {

    return "forbidden";

  }

  if (reason === "timeout") {

    return "timeout";

  }

  if (reason === "network_error") {

    return "network_error";

  }

  if (reason === "browser_unavailable") {

    return "browser_unavailable";

  }

  if (reason === "browser_failed") {

    return "browser_failed";

  }

  if (reason === "upstream_error") {

    return "api_unavailable";

  }

  return "preview_failed";

}

async function handleExternalUrlImportRequest(req, res, options = {}) {

  try {

    const body = normalizeRequestBodyObject(req.body);
    const requestedCreateQueueItem = hasOwn(body, "create_queue_item")
      ? normalizeOptionalBoolean(body.create_queue_item)
      : false;
    const createQueueItem = typeof options.createQueueItem === "boolean"
      ? options.createQueueItem
      : requestedCreateQueueItem;
    const requestedUserId = hasOwn(body, "user_id")
      ? normalizeOptionalPositiveIntegerString(body.user_id)
      : null;

    if (hasOwn(body, "create_queue_item") && requestedCreateQueueItem == null) {

      return sendExternalUrlImportFailure(
        res,
        400,
        "validation_failed",
        "create_queue_item must be a boolean"
      );

    }

    if (hasOwn(body, "user_id") && requestedUserId == null) {

      return sendExternalUrlImportFailure(
        res,
        400,
        "validation_failed",
        "user_id must be a positive integer"
      );

    }

    const owner = resolveQueueIngestionOwnerUserId(req, requestedUserId);

    if (owner.error) {

      return sendExternalUrlImportFailure(
        res,
        400,
        "validation_failed",
        owner.error
      );

    }

    const extraction = await externalProductImportService.importFromUrl({
      url: body.url,
      providerHint: body.provider_hint || body.provider,
      sourceLabel: body.source_label,
    });
    const normalized = validateAndNormalizeProductImportPayload(extraction.normalizedPayload);
    const validationErrors = [
      ...extraction.validation.errors,
      ...normalized.errors,
    ];
    const canCreateQueueItem = validationErrors.length === 0;

    if (!canCreateQueueItem) {

      const previewResponse = {
        ok: true,
        details: validationErrors,
        preview: extraction.preview,
        normalized_payload: extraction.normalizedPayload,
        validation: {
          can_create_queue_item: false,
          errors: validationErrors,
        },
        item: null,
        import: {
          owner_user_id: owner.userId,
          owner_scope_source: owner.scope_source,
          workflow_source: extraction.normalizedPayload.workflow_source,
          created_queue_item: false,
        },
      };

      if (!createQueueItem) {

        return res.json(previewResponse);

      }

      return sendExternalUrlImportFailure(
        res,
        422,
        "validation_failed",
        "External product extraction was incomplete",
        extraction.preview?.provider || null,
        {
          errors: validationErrors,
          preview: extraction.preview,
          normalized_payload: extraction.normalizedPayload,
        }
      );

    }

    let item = null;

    if (createQueueItem) {

      item = await createQueueItemFromPayload(owner.userId, normalized.value);

    }

    return res.json({
      ok: true,
      preview: extraction.preview,
      normalized_payload: normalized.value,
      validation: {
        can_create_queue_item: true,
        errors: [],
      },
      item: item ? serializeQueueItemForApi(item) : null,
      import: {
        owner_user_id: owner.userId,
        owner_scope_source: owner.scope_source,
        workflow_source: normalized.value.workflow_source,
        created_queue_item: Boolean(item),
      },
    });

  } catch (e) {

    if (e?.isInputError) {

      return sendExternalUrlImportFailure(
        res,
        400,
        e.code || "validation_failed",
        e.message,
        e.provider || null,
        e.details || null
      );

    }

    if (e?.isExtractionError) {

      return sendExternalUrlImportFailure(
        res,
        422,
        mapExternalUrlExtractionReasonToCode(e.reason),
        e.message,
        e.provider || null,
        {
          reason: e.reason || "extraction_failed",
          attempts: Array.isArray(e.attempts) ? e.attempts : [],
          ...(e.details != null ? { extraction_details: e.details } : {}),
        }
      );

    }

    return sendExternalUrlImportFailure(
      res,
      500,
      "api_unavailable",
      "Server error",
      null,
      e.message ? { error: e.message } : null
    );

  }

}

app.post("/imports/external-url", queueAuth, async (req, res) => {

  return handleExternalUrlImportRequest(req, res);

});

app.post("/internal/import-products/external-url/preview", queueAuth, async (req, res) => {

  return handleExternalUrlImportRequest(req, res, { createQueueItem: false });

});

app.post("/internal/import-products/external-url/import", queueAuth, async (req, res) => {

  return handleExternalUrlImportRequest(req, res, { createQueueItem: true });

});

app.get("/webhooks/endpoints", queueAuth, async (req, res) => {

  try {

    const owner = resolveQueueIngestionOwnerUserId(req, resolveQueueUserId(req));

    if (owner.error) {

      return res.status(400).json({ error: owner.error });

    }

    const endpoints = await pool.query(
      `
      SELECT *
      FROM inbound_webhook_endpoints
      WHERE user_id = $1
      ORDER BY id DESC
      `,
      [owner.userId]
    );

    return res.json({
      items: endpoints.rows.map((row) => sanitizeWebhookEndpoint(row, req)),
    });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.post("/webhooks/endpoints", queueAuth, async (req, res) => {

  try {

    const normalized = validateAndNormalizeWebhookEndpointPayload(req.body);

    if (normalized.errors.length > 0) {

      return res.status(400).json({ error: "Invalid webhook endpoint payload", details: normalized.errors });

    }

    const owner = resolveQueueIngestionOwnerUserId(
      req,
      normalized.value.requested_user_id
    );

    if (owner.error) {

      return res.status(400).json({ error: owner.error });

    }

    const created = await createWebhookEndpoint(req, owner.userId, normalized.value);

    return res.json({
      ok: true,
      endpoint: sanitizeWebhookEndpoint(created.endpoint, req),
      signing_secret: created.signing_secret,
    });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.post("/webhooks/endpoints/:id", queueAuth, async (req, res) => {

  try {

    const owner = resolveQueueIngestionOwnerUserId(req, resolveQueueUserId(req));
    const id = Number(req.params.id);
    const body = normalizeRequestBodyObject(req.body);
    const name = hasOwn(body, "name") ? normalizeOptionalString(body.name) : null;
    const sourceLabel = hasOwn(body, "source_label")
      ? normalizeOptionalString(body.source_label)
      : null;
    const active = hasOwn(body, "active") ? normalizeOptionalBoolean(body.active) : null;

    if (owner.error) {

      return res.status(400).json({ error: owner.error });

    }

    if (!Number.isFinite(id)) {

      return res.status(400).json({ error: "Invalid id" });

    }

    if (hasOwn(body, "active") && active == null) {

      return res.status(400).json({ error: "active must be a boolean" });

    }

    if (!hasOwn(body, "name") && !hasOwn(body, "source_label") && !hasOwn(body, "active")) {

      return res.status(400).json({ error: "Provide at least one of name, source_label, or active" });

    }

    const updated = await pool.query(
      `
      UPDATE inbound_webhook_endpoints
      SET name = CASE WHEN $3 THEN $4 ELSE name END,
          source_label = CASE WHEN $5 THEN $6 ELSE source_label END,
          active = CASE WHEN $7 THEN $8 ELSE active END,
          updated_at = NOW()
      WHERE id = $1 AND user_id = $2
      RETURNING *
      `,
      [
        id,
        owner.userId,
        hasOwn(body, "name"),
        name,
        hasOwn(body, "source_label"),
        sourceLabel,
        hasOwn(body, "active"),
        active,
      ]
    );

    if (updated.rowCount === 0) {

      return res.status(404).json({ error: "Webhook endpoint not found" });

    }

    return res.json({ ok: true, endpoint: sanitizeWebhookEndpoint(updated.rows[0], req) });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.get("/webhooks/endpoints/:id/deliveries", queueAuth, async (req, res) => {

  try {

    const owner = resolveQueueIngestionOwnerUserId(req, resolveQueueUserId(req));
    const id = Number(req.params.id);

    if (owner.error) {

      return res.status(400).json({ error: owner.error });

    }

    if (!Number.isFinite(id)) {

      return res.status(400).json({ error: "Invalid id" });

    }

    const deliveries = await pool.query(
      `
      SELECT d.id,
             d.endpoint_id,
             d.event,
             d.received_at,
             d.status,
             d.payload_summary,
             d.queue_item_id,
             d.error_message,
             d.created_at,
             d.updated_at
      FROM inbound_webhook_deliveries d
      INNER JOIN inbound_webhook_endpoints e ON e.id = d.endpoint_id
      WHERE d.endpoint_id = $1
        AND e.user_id = $2
      ORDER BY d.id DESC
      LIMIT 100
      `,
      [id, owner.userId]
    );

    return res.json({ items: deliveries.rows });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.post("/webhooks/inbound/:endpointKey", async (req, res) => {

  const endpointKey = normalizeOptionalString(req.params.endpointKey);
  let deliveryId = null;
  let endpointId = null;

  try {

    if (!endpointKey) {

      return res.status(404).json({ ok: false, error: "Webhook endpoint not found" });

    }

    const endpointResult = await pool.query(
      `
      SELECT *
      FROM inbound_webhook_endpoints
      WHERE endpoint_key = $1
      LIMIT 1
      `,
      [endpointKey]
    );

    if (endpointResult.rowCount === 0) {

      return res.status(404).json({ ok: false, error: "Webhook endpoint not found" });

    }

    const endpoint = endpointResult.rows[0];
    endpointId = endpoint.id;
    const deliverySeed = extractWebhookImportPayload(req.body);
    const delivery = await createWebhookDelivery(endpoint.id, deliverySeed.event, req.body);
    deliveryId = delivery.id;

    if (!endpoint.active) {

      await finalizeWebhookDelivery(delivery.id, {
        status: "ignored",
        error_message: "Webhook endpoint is inactive",
      });

      return res.status(409).json({
        ok: false,
        endpoint_id: endpoint.id,
        delivery_id: delivery.id,
        status: "ignored",
        error: "Webhook endpoint is inactive",
      });

    }

    const signatureCheck = verifyWebhookRequestSignature(req, endpoint);

    if (!signatureCheck.valid) {

      await finalizeWebhookDelivery(delivery.id, {
        status: "rejected",
        error_message: "Webhook signature validation failed",
      });

      return res.status(401).json({
        ok: false,
        endpoint_id: endpoint.id,
        delivery_id: delivery.id,
        status: "rejected",
        error: "Webhook signature validation failed",
      });

    }

    if (deliverySeed.event !== DEFAULT_WEBHOOK_EVENT) {

      await finalizeWebhookDelivery(delivery.id, {
        status: "rejected",
        error_message: `Unsupported event: ${deliverySeed.event}`,
      });

      return res.status(400).json({
        ok: false,
        endpoint_id: endpoint.id,
        delivery_id: delivery.id,
        status: "rejected",
        error: `Unsupported event: ${deliverySeed.event}`,
      });

    }

    const importPayload = {
      ...deliverySeed.payload,
      workflow_source:
        normalizeOptionalString(deliverySeed.payload.workflow_source) ||
        "webhook_inbound",
    };
    const normalized = validateAndNormalizeProductImportPayload(importPayload);

    if (normalized.errors.length > 0) {

      await finalizeWebhookDelivery(delivery.id, {
        status: "failed",
        error_message: normalized.errors.join("; "),
      });

      return res.status(400).json({
        ok: false,
        endpoint_id: endpoint.id,
        delivery_id: delivery.id,
        status: "failed",
        error: "Invalid import payload",
        details: normalized.errors,
      });

    }

    const item = await createQueueItemFromPayload(endpoint.user_id, normalized.value);

    await finalizeWebhookDelivery(delivery.id, {
      status: "processed",
      queue_item_id: item.id,
    });

    return res.json({
      ok: true,
      endpoint_id: endpoint.id,
      delivery_id: delivery.id,
      queue_item_id: item.id,
      status: "processed",
    });

  } catch (e) {

    if (deliveryId != null) {

      try {

        await finalizeWebhookDelivery(deliveryId, {
          status: "failed",
          error_message: e.message,
        });

      } catch {}

    }

    return res.status(500).json({
      ok: false,
      endpoint_id: endpointId,
      delivery_id: deliveryId,
      error: "Server error",
      message: e.message,
    });

  }

});



// ========================

// Protected: Queue List

// ========================



app.get("/queue/list", queueAuth, async (req, res) => {

  try {

    const userId = req.user.id;



    const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 100);

    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    const status = req.query.status;

    console.log("[queue-debug] queue list request mode", {
      user_id: userId,
      status: status || null,
      limit,
      offset,
      mode: "dashboard_full_history",
    });



    let query = `

      SELECT *

      FROM products_queue

      WHERE user_id = $1

    `;



    const values = [userId];

    let index = 2;



    if (status) {

      query += ` AND status = $${index}`;

      values.push(status);

      index++;

    }



    query += `

      ORDER BY created_at DESC

      LIMIT $${index}

      OFFSET $${index + 1}

    `;



    values.push(limit, offset);



    const r = await pool.query(query, values);



    const total = await pool.query(

      `SELECT COUNT(*) FROM products_queue WHERE user_id=$1`,

      [userId]

    );

    const statusesIncluded = Array.from(
      new Set(r.rows.map((row) => row.status).filter((value) => value != null))
    );

    console.log("[queue-debug] returned row count", {
      user_id: userId,
      returned: r.rowCount,
      total: Number(total.rows[0].count),
    });

    console.log("[queue-debug] statuses included in dashboard list", {
      user_id: userId,
      statuses: statusesIncluded,
    });



    return res.json({

      total: Number(total.rows[0].count),

      limit,

      offset,

      items: r.rows.map((row) => serializeQueueItemForApi(row)),

    });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.get("/queue/:id", queueAuth, async (req, res) => {

  try {

    const userId = req.user.id;
    const id = Number(req.params.id);

    if (!Number.isFinite(id)) {
      return res.status(400).json({ error: "Invalid id" });
    }

    const r = await pool.query(
      `
      SELECT *
      FROM products_queue
      WHERE id = $1 AND user_id = $2
      LIMIT 1
      `,
      [id, userId]
    );

    if (r.rowCount === 0) {
      return res.status(404).json({ error: "Queue item not found" });
    }

    return res.json({ item: serializeQueueItemForApi(r.rows[0]) });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});



// ========================

// Protected: Queue Next

// ========================



app.get("/queue/next", queueAuth, async (req, res) => {

  try {

    const userId = req.user.id;



    const r = await pool.query(

      `

      SELECT *

      FROM products_queue

      WHERE user_id=$1

        AND status='scheduled'

        AND next_run_at <= NOW()

      ORDER BY next_run_at ASC, id ASC

      LIMIT 1

      `,

      [userId]

    );



    return res.json({
      item: r.rowCount ? serializeQueueItemForApi(r.rows[0]) : null,
    });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});



// ========================

// Protected: Queue Mark Done

// ========================



app.post("/queue/mark-done/:id", queueAuth, async (req, res) => {

  try {

    const userId = req.user.id;

    const id = Number(req.params.id);



    if (!Number.isFinite(id)) {

      return res.status(400).json({ error: "Invalid id" });

    }



    const r = await pool.query(

      `

      UPDATE products_queue

      SET status='done', updated_at=NOW()

      WHERE id=$1 AND user_id=$2

      RETURNING *

      `,

      [id, userId]

    );



    if (r.rowCount === 0) {

      return res.status(404).json({ error: "Queue item not found" });

    }



    return res.json({ item: r.rows[0] });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.post("/queue/update/:id", queueUpdateTextParser, queueAuth, async (req, res) => {

  try {

    const userId = req.user.id;

    const id = Number(req.params.id);

    const body = normalizeRequestBodyObject(req.body);
    const status = normalizeOptionalString(body.status);
    const publishCount = normalizeOptionalInteger(body.publish_count);
    const maxRepublish = normalizeOptionalInteger(body.max_republish);
    const maxAttempts = normalizeOptionalInteger(body.max_attempts);
    const nextRunAt = normalizeOptionalTimestamp(body.next_run_at);
    const lastTitle = normalizeOptionalString(body.last_title);
    const lastDescription = normalizeOptionalString(body.last_description);
    const lastCaption = normalizeOptionalString(body.last_caption);
    const lastImage = normalizeOptionalString(body.last_image);
    const hashtags = normalizeOptionalString(body.hashtags);
    const deleteRequested =
      hasOwn(body, "status") && String(status || "").trim().toLowerCase() === "deleted";



    if (!Number.isFinite(id)) {

      return res.status(400).json({ error: "Invalid id" });

    }

    if (deleteRequested) {

      console.log("[queue-debug] delete request input", {
        id,
        user_id: userId,
        status,
        body_keys: Object.keys(body),
      });

      console.log("[queue-debug] delete SQL path chosen", {
        endpoint: "/queue/update/:id",
        path: "hard_delete",
      });

      const deleted = await pool.query(
        `
        DELETE FROM products_queue
        WHERE id=$1 AND user_id=$2
        RETURNING *
        `,
        [id, userId]
      );

      console.log("[queue-debug] delete affected row count", {
        id,
        row_count: deleted.rowCount,
      });

      if (deleted.rowCount === 0) {

        const payload = { error: "Queue item not found", deleted: false };
        console.log("[queue-debug] final response payload", payload);
        return res.status(404).json(payload);

      }

      const payload = { deleted: true, item: deleted.rows[0] };
      console.log("[queue-debug] final response payload", payload);
      return res.json(payload);

    }

    if (hasOwn(body, "publish_count") && publishCount == null) {

      return res.status(400).json({ error: "publish_count must be an integer" });

    }

    if (hasOwn(body, "max_republish") && (maxRepublish == null || maxRepublish < 0)) {

      return res.status(400).json({ error: "max_republish must be an integer >= 0" });

    }

    if (hasOwn(body, "max_attempts") && (maxAttempts == null || maxAttempts < 1)) {

      return res.status(400).json({ error: "max_attempts must be an integer >= 1" });

    }

    if (hasOwn(body, "next_run_at") && nextRunAt == null) {

      return res.status(400).json({ error: "next_run_at must be a valid timestamp" });

    }



    const r = await pool.query(

      `

      UPDATE products_queue

      SET status=CASE WHEN $2 THEN $3 ELSE status END,

          publish_count=CASE WHEN $4 THEN $5 ELSE publish_count END,

          max_republish=CASE WHEN $6 THEN $7 ELSE max_republish END,

          max_attempts=CASE WHEN $8 THEN $9 ELSE max_attempts END,

          next_run_at=CASE WHEN $10 THEN $11 ELSE next_run_at END,

          last_title=CASE WHEN $12 THEN $13 ELSE last_title END,

          last_description=CASE WHEN $14 THEN $15 ELSE last_description END,

          last_caption=CASE WHEN $16 THEN $17 ELSE last_caption END,

          last_image=CASE WHEN $18 THEN $19 ELSE last_image END,

          last_hashtags=CASE WHEN $20 THEN $21 ELSE last_hashtags END,

          updated_at=NOW()

      WHERE id=$1 AND user_id=$22

      RETURNING *

      `,

      [

        id,

        hasOwn(body, "status"),

        status,

        hasOwn(body, "publish_count"),

        publishCount,

        hasOwn(body, "max_republish"),

        maxRepublish,

        hasOwn(body, "max_attempts"),

        maxAttempts,

        hasOwn(body, "next_run_at"),

        nextRunAt,

        hasOwn(body, "last_title"),

        lastTitle,

        hasOwn(body, "last_description"),

        lastDescription,

        hasOwn(body, "last_caption"),

        lastCaption,

        hasOwn(body, "last_image"),

        lastImage,

        hasOwn(body, "hashtags"),

        hashtags,

        userId,

      ]

    );



    if (r.rowCount === 0) {

      return res.status(404).json({ error: "Queue item not found" });

    }



    return res.json({ item: r.rows[0] });

  } catch (e) {

    return res.status(500).json({ error: "Server error", message: e.message });

  }

});

app.post("/queue/process/:id", queueAuth, async (req, res) => {

  const id = Number(req.params.id);

  const userId = req.user.id;

  const {

    dry_run,

    title,

    description,

    caption,

    image,

    hashtags,

    publish_count,

    max_republish,

  } = req.body || {};



  if (!Number.isFinite(id)) {

    return res.status(400).json({ error: "Invalid id" });

  }



  const client = await pool.connect();

  try {

    await client.query("BEGIN");



    const row = await client.query(

      `

      SELECT *

      FROM products_queue

      WHERE id=$1 AND user_id=$2

      FOR UPDATE

      `,

      [id, userId]

    );



    if (row.rowCount === 0) {

      await client.query("ROLLBACK");

      return res.status(404).json({ error: "Queue item not found" });

    }



    const item = row.rows[0];

    const publishDecision = getQueuePublishDecision(item);

    console.log("[queue-debug] duplicate publish prevention decision", {
      source: "queue_process",
      id: item.id,
      product_id: item.product_id,
      status: item.status,
      publish_count: publishDecision.currentPublishCount,
      max_republish: publishDecision.effectiveMaxRepublish,
      next_run_at: publishDecision.nextRunAt,
      is_due_now: publishDecision.isDueNow,
      eligible: publishDecision.eligible,
      reason: publishDecision.reason,
    });

    if (!publishDecision.eligible) {

      await client.query("ROLLBACK");

      return res.status(409).json({
        error: "Queue item is not eligible for publish",
        decision: publishDecision,
      });

    }



    if (dry_run === true) {

      const currentPublishCount = publishDecision.currentPublishCount;

      const nextPublishCount = currentPublishCount + 1;

      const effectiveMaxRepublish = Number.isInteger(max_republish)
        ? max_republish
        : publishDecision.effectiveMaxRepublish;

      const shouldContinueReposting = nextPublishCount < effectiveMaxRepublish;

      const nextStatus = nextPublishCount >= effectiveMaxRepublish ? "published" : "scheduled";

      const nextRunAt = shouldContinueReposting
        ? computeNextQueueRunAt(nextPublishCount)
        : new Date().toISOString();

      console.log("[queue-debug] publish start", {
        source: "queue_process",
        id: item.id,
        product_id: item.product_id,
        publish_count_before: currentPublishCount,
        max_republish: effectiveMaxRepublish,
      });

      if (nextPublishCount === 1) {

        console.log("[queue-debug] item completion after first publish", {
          id: item.id,
          product_id: item.product_id,
          publish_count: nextPublishCount,
          max_republish: effectiveMaxRepublish,
          status_after_publish: nextStatus,
        });

      }

      console.log("[queue-debug] next_run_at reassignment", {
        id: item.id,
        product_id: item.product_id,
        publish_count: nextPublishCount,
        max_republish: effectiveMaxRepublish,
        next_run_at: nextRunAt,
        repost_continues: shouldContinueReposting,
      });



      const updated = await client.query(

        `

        UPDATE products_queue

        SET publish_count=$2,

            last_title=$3,

            last_description=$4,

            last_caption=$5,

            last_image=$6,

            last_hashtags=$7,

            status=$8,

            attempts=0,

            next_run_at=$9,

            last_error=NULL,

            last_error_at=NULL,

            processing_started_at=NULL,

            locked_by=NULL,

            locked_at=NULL,

            lease_expires_at=NULL,

            updated_at=NOW()

        WHERE id=$1 AND user_id=$10

        RETURNING *

        `,

        [

          id,

          nextPublishCount,

          title || null,

          description || null,

          caption || null,

          image || null,

          hashtags || null,

          nextStatus,

          nextRunAt,

          userId,

        ]

      );



      await client.query("COMMIT");

      console.log("[queue-debug] publish success", {
        source: "queue_process",
        id: updated.rows[0]?.id ?? item.id,
        product_id: updated.rows[0]?.product_id ?? item.product_id,
        publish_count: updated.rows[0]?.publish_count ?? nextPublishCount,
        status: updated.rows[0]?.status ?? nextStatus,
      });

      return res.json({ item: updated.rows[0] });

    }



    await client.query("COMMIT");

    return res.json({

      mode: "real_publish_not_implemented",

      item,

      payload: {

        title: title || null,

        description: description || null,

        caption: caption || null,

        image: image || null,

        hashtags: hashtags || null,

        publish_count: Number.isInteger(publish_count) ? publish_count : null,

        max_republish: Number.isInteger(max_republish) ? max_republish : null,

      },

    });

  } catch (e) {

    await client.query("ROLLBACK");

    return res.status(500).json({ error: "Server error", message: e.message });

  } finally {

    client.release();

  }

});

// ========================

// Worker Auth

// ========================



function workerAuth(req, res, next) {

  const token = (req.headers["x-worker-token"] || "").trim();

  if (!token || token !== (process.env.WORKER_TOKEN || "")) {

    return res.status(401).json({ error: "Unauthorized worker" });

  }

  next();

}

function isNumericRouteParam(value) {
  return typeof value === "string" && /^[0-9]+$/.test(value.trim());
}

function repostNumericIdRouteGuard(action) {
  return (req, res, next) => {
    const rawId = typeof req.params?.id === "string" ? req.params.id.trim() : "";
    if (isNumericRouteParam(rawId)) {
      return next();
    }

    logInternalRepost("info", "route_passthrough_non_numeric_id", {
      action,
      processing_engine: "internal",
      matched_route: req.route?.path || null,
      original_url: req.originalUrl || req.url || null,
      method: req.method || null,
      raw_id: rawId || null,
    });
    return next("route");
  };
}

function logManualTestRouteDebug(eventName, action, req, details = {}) {
  logInternalRepost("info", eventName, {
    action,
    processing_engine: "internal",
    matched_route: req.route?.path || null,
    original_url: req.originalUrl || req.url || null,
    method: req.method || null,
    request_body: normalizeRequestBodyObject(req.body),
    ...details,
  });
}



function validatePublishBody(body) {

  const errors = [];

  const platform =
    typeof body?.platform === "string" ? body.platform.trim().toLowerCase() : "";
  const socialAccountIdProvided = hasOwn(body || {}, "social_account_id");
  const socialAccountId = socialAccountIdProvided
    ? normalizeOptionalInteger(body?.social_account_id)
    : null;
  const productIdRaw = body?.product_id;
  const productId =
    typeof productIdRaw === "string" || typeof productIdRaw === "number"
      ? String(productIdRaw).trim()
      : "";
  const url = typeof body?.url === "string" ? body.url.trim() : "";
  const image = typeof body?.image === "string" ? body.image.trim() : "";
  const imageUrl =
    typeof body?.image_url === "string" ? body.image_url.trim() : "";
  const title = typeof body?.title === "string" ? body.title.trim() : "";
  const description =
    typeof body?.description === "string" ? body.description.trim() : "";
  const caption = typeof body?.caption === "string" ? body.caption.trim() : "";
  const hashtags =
    typeof body?.hashtags === "string"
      ? body.hashtags.trim()
      : Array.isArray(body?.hashtags)
        ? body.hashtags
            .filter((value) => typeof value === "string" && value.trim())
            .map((value) => value.trim())
            .join(" ")
        : "";

  if (!platform) {

    errors.push("platform is required");

  }

  if (!productId) {

    errors.push("product_id is required");

  }

  if (socialAccountIdProvided && (!Number.isInteger(socialAccountId) || socialAccountId <= 0)) {

    errors.push("social_account_id must be a positive integer");

  }

  if (!url && !image && !imageUrl) {

    errors.push("image_url, image, or url is required");

  }

  if (platform && platform !== "instagram") {

    errors.push("platform must be instagram");

  }

  const normalizedImageCandidate = imageUrl || image || url;
  let normalizedImageUrl = "";
  const safeCaption = buildInstagramSafeCaption({
    caption,
    title,
    description,
    hashtags,
  });

  if (normalizedImageCandidate) {

    const normalizedImageUrlResult = normalizeInstagramMediaUrl(
      normalizedImageCandidate
    );

    if (!normalizedImageUrlResult.ok) {

      errors.push(normalizedImageUrlResult.error);

    } else {

      normalizedImageUrl = normalizedImageUrlResult.value;

    }

  }

  return {
    errors,
    value: {
      platform,
      social_account_id: socialAccountId,
      product_id: productId,
      url,
      title: sanitizeInstagramCaptionText(title),
      description: sanitizeInstagramCaptionText(description),
      caption: safeCaption.caption,
      image,
      image_url: imageUrl,
      normalized_image_url: normalizedImageUrl,
      hashtags: normalizeInstagramHashtags(hashtags),
      caption_length: safeCaption.caption_length,
      caption_was_truncated: safeCaption.was_truncated,
      caption_components: safeCaption.components,
    },
  };

}

function parsePngDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 24) {
    return null;
  }

  const pngSignature = "89504e470d0a1a0a";
  if (buffer.subarray(0, 8).toString("hex") !== pngSignature) {
    return null;
  }

  return {
    format: "png",
    width: buffer.readUInt32BE(16),
    height: buffer.readUInt32BE(20),
  };
}

function parseGifDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 10) {
    return null;
  }

  const signature = buffer.subarray(0, 6).toString("ascii");
  if (signature !== "GIF87a" && signature !== "GIF89a") {
    return null;
  }

  return {
    format: "gif",
    width: buffer.readUInt16LE(6),
    height: buffer.readUInt16LE(8),
  };
}

function parseWebpDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 30) {
    return null;
  }

  if (
    buffer.subarray(0, 4).toString("ascii") !== "RIFF" ||
    buffer.subarray(8, 12).toString("ascii") !== "WEBP"
  ) {
    return null;
  }

  const chunkType = buffer.subarray(12, 16).toString("ascii");

  if (chunkType === "VP8X" && buffer.length >= 30) {
    const width = 1 + buffer.readUIntLE(24, 3);
    const height = 1 + buffer.readUIntLE(27, 3);
    return { format: "webp", width, height };
  }

  if (chunkType === "VP8L" && buffer.length >= 25) {
    const bits = buffer.readUInt32LE(21);
    const width = (bits & 0x3fff) + 1;
    const height = ((bits >> 14) & 0x3fff) + 1;
    return { format: "webp", width, height };
  }

  if (
    chunkType === "VP8 " &&
    buffer.length >= 30 &&
    buffer[23] === 0x9d &&
    buffer[24] === 0x01 &&
    buffer[25] === 0x2a
  ) {
    const width = buffer.readUInt16LE(26) & 0x3fff;
    const height = buffer.readUInt16LE(28) & 0x3fff;
    return { format: "webp", width, height };
  }

  return null;
}

function parseJpegDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 4) {
    return null;
  }

  if (buffer[0] !== 0xff || buffer[1] !== 0xd8) {
    return null;
  }

  let offset = 2;
  while (offset + 3 < buffer.length) {
    if (buffer[offset] !== 0xff) {
      offset += 1;
      continue;
    }

    while (offset < buffer.length && buffer[offset] === 0xff) {
      offset += 1;
    }

    if (offset >= buffer.length) {
      break;
    }

    const marker = buffer[offset];
    offset += 1;

    if (
      marker === 0x01 ||
      (marker >= 0xd0 && marker <= 0xd9)
    ) {
      continue;
    }

    if (offset + 1 >= buffer.length) {
      break;
    }

    const segmentLength = buffer.readUInt16BE(offset);
    if (segmentLength < 2 || offset + segmentLength > buffer.length) {
      break;
    }

    const isStartOfFrame = [
      0xc0, 0xc1, 0xc2, 0xc3,
      0xc5, 0xc6, 0xc7,
      0xc9, 0xca, 0xcb,
      0xcd, 0xce, 0xcf,
    ].includes(marker);

    if (isStartOfFrame && segmentLength >= 7) {
      return {
        format: "jpeg",
        height: buffer.readUInt16BE(offset + 3),
        width: buffer.readUInt16BE(offset + 5),
      };
    }

    offset += segmentLength;
  }

  return null;
}

function parseImageDimensions(buffer) {
  return (
    parsePngDimensions(buffer) ||
    parseGifDimensions(buffer) ||
    parseWebpDimensions(buffer) ||
    parseJpegDimensions(buffer)
  );
}

function computeImageAspectRatio(width, height) {
  if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {
    return null;
  }

  return width / height;
}

function formatImageAspectRatio(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) {
    return null;
  }

  return Number(ratio.toFixed(4));
}

function validateInstagramPhotoAspectRatio(width, height) {
  const aspectRatio = computeImageAspectRatio(width, height);
  if (!aspectRatio) {
    return {
      accepted: false,
      aspectRatio: null,
      reason: "Image dimensions must be positive integers",
    };
  }

  const accepted =
    aspectRatio >= INSTAGRAM_PHOTO_ASPECT_RATIO_MIN &&
    aspectRatio <= INSTAGRAM_PHOTO_ASPECT_RATIO_MAX;

  return {
    accepted,
    aspectRatio,
    reason: accepted
      ? null
      : `Instagram photo aspect ratio must be between ${INSTAGRAM_PHOTO_ASPECT_RATIO_MIN.toFixed(2)} and ${INSTAGRAM_PHOTO_ASPECT_RATIO_MAX.toFixed(2)}`,
  };
}

async function fetchImageMetadata(imageUrl) {
  const response = await axios.get(imageUrl, {
    responseType: "arraybuffer",
    timeout: INSTAGRAM_IMAGE_METADATA_TIMEOUT_MS,
    maxRedirects: 5,
    validateStatus: (status) => status >= 200 && status < 400,
  });

  const contentType = normalizeOptionalString(response.headers?.["content-type"]) || "";
  if (!contentType.toLowerCase().startsWith("image/")) {
    throw new Error(`Remote URL returned non-image content-type: ${contentType || "unknown"}`);
  }

  const buffer = Buffer.from(response.data);
  const dimensions = parseImageDimensions(buffer);
  if (!dimensions?.width || !dimensions?.height) {
    throw new Error("Could not determine remote image width and height");
  }

  return {
    width: dimensions.width,
    height: dimensions.height,
    format: dimensions.format,
    contentType,
    finalUrl:
      response?.request?.res?.responseUrl ||
      response?.config?.url ||
      imageUrl,
  };
}

function buildInstagramPublishImageCandidates(prepared, payload) {
  const seen = new Set();
  const output = [];
  const selectedImage = normalizeOptionalString(
    payload?.selected_image ||
      payload?.image_url ||
      payload?.image ||
      prepared?.selected_image
  );
  const images = normalizeQueueImages(prepared?.images);

  function pushCandidate(originalValue, source) {
    const original = normalizeOptionalString(originalValue);
    if (!original) {
      return;
    }

    const normalizedResult = normalizeInstagramMediaUrl(original);
    if (!normalizedResult.ok || !normalizedResult.value) {
      output.push({
        source,
        original,
        normalized: null,
        invalidReason: normalizedResult.error || "image_url is invalid",
      });
      return;
    }

    const dedupeKey = normalizedResult.value;
    if (seen.has(dedupeKey)) {
      return;
    }

    seen.add(dedupeKey);
    output.push({
      source,
      original,
      normalized: normalizedResult.value,
      invalidReason: null,
    });
  }

  pushCandidate(selectedImage, "selected_image");
  if (Array.isArray(images)) {
    for (const image of images) {
      pushCandidate(image, "images");
    }
  }

  return output;
}

function applyValidatedInstagramImageSelection(prepared, payload, candidate, metadata, decision) {
  const aspectRatio = computeImageAspectRatio(metadata.width, metadata.height);
  prepared.selected_image = candidate.original;
  prepared.selected_image_source =
    decision === "replaced" ? "image_validation_replacement" : prepared.selected_image_source;
  payload.image = candidate.original;
  payload.image_url = candidate.original;
  payload.selected_image = candidate.original;
  payload.normalized_image_url = candidate.normalized;
  payload.image_validation = {
    accepted: true,
    decision,
    source: candidate.source,
    width: metadata.width,
    height: metadata.height,
    aspect_ratio: formatImageAspectRatio(aspectRatio),
    format: metadata.format,
    content_type: metadata.contentType,
    final_url: metadata.finalUrl,
  };
}

function createInstagramImageValidationError(payload, attempts) {
  const attemptedSummary = attempts
    .map((attempt) => {
      const size =
        Number.isFinite(attempt.width) && Number.isFinite(attempt.height)
          ? `${attempt.width}x${attempt.height}`
          : "unknown-size";
      return `${attempt.original} (${size}; ${attempt.reason})`;
    })
    .join("; ");

  const message = attemptedSummary
    ? `No Instagram-compatible image available. ${attemptedSummary}`
    : "No Instagram-compatible image available";

  return createPublishError(
    message,
    "UNSUPPORTED_ASPECT_RATIO",
    "validation_error",
    attemptedSummary || undefined,
    Number.isInteger(payload?.social_account_id) && payload.social_account_id > 0
      ? payload.social_account_id
      : null
  );
}

async function validateInstagramPublishImageSelection(prepared, payload) {
  const candidates = buildInstagramPublishImageCandidates(prepared, payload);
  const initiallySelectedImage = normalizeOptionalString(prepared?.selected_image);
  const attempts = [];

  for (const candidate of candidates) {
    if (candidate.invalidReason) {
      console.log("[instagram] image_validation", {
        queue_item_id: payload?.queue_item_id ?? prepared?.queue_item_id ?? null,
        product_id: payload?.product_id ?? prepared?.product_id ?? null,
        initial_selected_image: initiallySelectedImage,
        selected_image: candidate.original,
        image_width: null,
        image_height: null,
        aspect_ratio: null,
        decision: "skipped",
        reason: candidate.invalidReason,
      });
      attempts.push({
        original: candidate.original,
        width: null,
        height: null,
        reason: candidate.invalidReason,
      });
      continue;
    }

    try {
      const metadata = await fetchImageMetadata(candidate.normalized);
      const aspect = validateInstagramPhotoAspectRatio(metadata.width, metadata.height);
      const decision =
        candidate.original === initiallySelectedImage ? "accepted" : "replaced";

      console.log("[instagram] image_validation", {
        queue_item_id: payload?.queue_item_id ?? prepared?.queue_item_id ?? null,
        product_id: payload?.product_id ?? prepared?.product_id ?? null,
        initial_selected_image: initiallySelectedImage,
        selected_image: candidate.original,
        image_width: metadata.width,
        image_height: metadata.height,
        aspect_ratio: formatImageAspectRatio(aspect.aspectRatio),
        decision: aspect.accepted ? decision : "skipped",
        reason: aspect.reason,
      });

      if (aspect.accepted) {
        applyValidatedInstagramImageSelection(prepared, payload, candidate, metadata, decision);
        return {
          ok: true,
          selectedImage: candidate.original,
          replaced: decision === "replaced",
        };
      }

      attempts.push({
        original: candidate.original,
        width: metadata.width,
        height: metadata.height,
        reason: aspect.reason || "Unsupported aspect ratio",
      });
    } catch (error) {
      const reason = error?.message || "Failed to inspect remote image";
      console.log("[instagram] image_validation", {
        queue_item_id: payload?.queue_item_id ?? prepared?.queue_item_id ?? null,
        product_id: payload?.product_id ?? prepared?.product_id ?? null,
        initial_selected_image: initiallySelectedImage,
        selected_image: candidate.original,
        image_width: null,
        image_height: null,
        aspect_ratio: null,
        decision: "skipped",
        reason,
      });
      attempts.push({
        original: candidate.original,
        width: null,
        height: null,
        reason,
      });
    }
  }

  return {
    ok: false,
    error: createInstagramImageValidationError(payload, attempts),
  };
}

function getInstagramPublishConfig() {

  const graphVersion = (
    process.env.META_GRAPH_VERSION || DEFAULT_META_GRAPH_VERSION
  ).trim();
  const timeoutMs = Number(
    process.env.META_GRAPH_TIMEOUT_MS || DEFAULT_META_TIMEOUT_MS
  );
  const containerPollIntervalMs = Number(
    process.env.META_CONTAINER_POLL_INTERVAL_MS ||
      DEFAULT_META_CONTAINER_POLL_INTERVAL_MS
  );
  const containerPollMaxAttempts = Number(
    process.env.META_CONTAINER_POLL_MAX_ATTEMPTS ||
      DEFAULT_META_CONTAINER_POLL_MAX_ATTEMPTS
  );

  return {
    graphVersion,
    timeoutMs:
      Number.isFinite(timeoutMs) && timeoutMs > 0
        ? timeoutMs
        : DEFAULT_META_TIMEOUT_MS,
    containerPollIntervalMs:
      Number.isFinite(containerPollIntervalMs) && containerPollIntervalMs > 0
        ? containerPollIntervalMs
        : DEFAULT_META_CONTAINER_POLL_INTERVAL_MS,
    containerPollMaxAttempts:
      Number.isInteger(containerPollMaxAttempts) && containerPollMaxAttempts > 0
        ? containerPollMaxAttempts
        : DEFAULT_META_CONTAINER_POLL_MAX_ATTEMPTS,
  };

}


function logInstagramPublishConfigStatus() {

  const {
    graphVersion,
    timeoutMs,
    containerPollIntervalMs,
    containerPollMaxAttempts,
  } = getInstagramPublishConfig();

  console.log(
    `[instagram] publish integration enabled (graph=${graphVersion}, timeout_ms=${timeoutMs}, poll_interval_ms=${containerPollIntervalMs}, poll_max_attempts=${containerPollMaxAttempts}, auth=per_social_account)`
  );

}

function createPublishError(
  message,
  code,
  type,
  details = undefined,
  socialAccountId = null
) {

  return {
    success: false,
    platform: "instagram",
    social_account_id: socialAccountId,
    publish_result: null,
    error: {
      message,
      code: code == null ? null : String(code),
      type: type || "publish_error",
      ...(details ? { details } : {}),
    },
  };

}

function normalizeConfirmedInstagramPublishResponse(publishResponse) {

  if (!publishResponse || publishResponse.success !== true) {

    return null;

  }

  const externalId = String(
    publishResponse?.publish_result?.external_id || ""
  ).trim();
  const permalink = String(
    publishResponse?.publish_result?.url || ""
  ).trim();
  const publishedAt = String(
    publishResponse?.publish_result?.published_at || ""
  ).trim();

  if (!externalId || !permalink || !publishedAt) {

    return null;

  }

  if (!/https?:\/\/(www\.)?instagram\.com\//i.test(permalink)) {

    return null;

  }

  return {
    externalId,
    permalink,
    publishedAt,
  };

}

function serializeJsonForLog(value) {

  if (value == null) {

    return null;

  }

  if (typeof value === "string") {

    try {

      return JSON.stringify(JSON.parse(value));

    } catch {

      return JSON.stringify(value);

    }

  }

  const seen = new WeakSet();

  try {

    return JSON.stringify(value, (key, nestedValue) => {
      if (typeof nestedValue === "bigint") {
        return nestedValue.toString();
      }

      if (nestedValue && typeof nestedValue === "object") {
        if (seen.has(nestedValue)) {
          return "[Circular]";
        }
        seen.add(nestedValue);
      }

      return nestedValue;
    });

  } catch {

    return null;

  }

}

function buildInstagramApiFailureContext(stage, error, request = {}) {

  return {
    stage,
    http_status: error?.response?.status ?? null,
    error_code: error?.code || null,
    request,
    response_body: serializeJsonForLog(error?.response?.data),
  };

}

function attachInstagramApiFailure(error, stage, request = {}) {

  if (!error || typeof error !== "object") {

    return error;

  }

  error.instagramApiFailure = buildInstagramApiFailureContext(stage, error, request);
  return error;

}

function extractInstagramApiFailure(error, fallbackStage = "unknown", request = {}) {

  return (
    error?.instagramApiFailure ||
    buildInstagramApiFailureContext(fallbackStage, error, request)
  );

}


function normalizeMetaError(error, socialAccountId = null) {

  const providerFailure = extractInstagramApiFailure(error, "publish_to_instagram");
  const metaError = error?.response?.data?.error;

  if (metaError) {

    const publishError = createPublishError(
      metaError.message || "Meta Graph API error",
      metaError.code ?? metaError.error_subcode ?? error?.response?.status ?? null,
      metaError.type || "meta_api_error",
      metaError.error_user_msg || undefined,
      socialAccountId
    );
    publishError.error.provider_failure = providerFailure;
    return publishError;

  }

  if (error?.code === "ECONNABORTED") {

    const publishError = createPublishError(
      "Meta Graph API request timed out",
      "TIMEOUT",
      "network_timeout",
      undefined,
      socialAccountId
    );
    publishError.error.provider_failure = providerFailure;
    return publishError;

  }

  const publishError = createPublishError(
    error?.message || "Instagram publish failed",
    error?.code || null,
    "network_error",
    undefined,
    socialAccountId
  );
  publishError.error.provider_failure = providerFailure;
  return publishError;

}


function createInstagramClient(config) {

  return axios.create({
    baseURL: `https://graph.facebook.com/${config.graphVersion}`,
    timeout: config.timeoutMs,
  });

}


async function createInstagramMediaContainer(client, config, payload) {

  const params = {
    image_url: payload.normalized_image_url,
    access_token: config.accessToken,
  };

  if (payload.caption) {

    params.caption = payload.caption;

  }

  console.log("[instagram] create_media_container request", {
    product_id: payload.product_id,
    social_account_id:
      Number.isInteger(payload?.social_account_id) && payload.social_account_id > 0
        ? payload.social_account_id
        : null,
    ig_user_id: config.igUserId,
    image_url: payload.normalized_image_url,
    caption_length: payload.caption ? payload.caption.length : 0,
  });

  try {

    const response = await client.post(`/${config.igUserId}/media`, null, {
      params,
    });

    return response?.data?.id;

  } catch (error) {

    throw attachInstagramApiFailure(error, "create_media_container", {
      ig_user_id: config.igUserId,
      image_url: payload.normalized_image_url,
      caption_length: payload.caption ? payload.caption.length : 0,
      caption_preview: payload.caption ? payload.caption.slice(0, 280) : "",
    });

  }

}

function sleep(ms) {

  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

}

async function fetchInstagramMediaContainerStatus(client, config, creationId) {

  try {

    const response = await client.get(`/${creationId}`, {
      params: {
        fields: "id,status,status_code",
        access_token: config.accessToken,
      },
    });

    return response?.data || null;

  } catch (error) {

    throw attachInstagramApiFailure(error, "fetch_media_container_status", {
      creation_id: creationId,
      ig_user_id: config.igUserId,
    });

  }

}

function normalizeInstagramContainerStatus(containerStatus) {

  return String(
    containerStatus?.status_code || containerStatus?.status || ""
  ).trim().toUpperCase();

}

function isInstagramContainerReady(status) {

  return status === "FINISHED" || status === "PUBLISHED";

}

function isInstagramContainerFailureStatus(status) {

  return ["ERROR", "EXPIRED", "FAILED"].includes(status);

}

async function waitForInstagramMediaContainerReady(client, config, creationId) {

  let lastContainerStatus = null;
  let lastStatus = "";

  for (let attempt = 1; attempt <= config.containerPollMaxAttempts; attempt += 1) {

    lastContainerStatus = await fetchInstagramMediaContainerStatus(
      client,
      config,
      creationId
    );
    lastStatus = normalizeInstagramContainerStatus(lastContainerStatus);

    if (isInstagramContainerReady(lastStatus)) {

      return {
        ready: true,
        attempt,
        status: lastStatus,
        containerStatus: lastContainerStatus,
      };

    }

    if (isInstagramContainerFailureStatus(lastStatus)) {

      return {
        ready: false,
        attempt,
        status: lastStatus,
        containerStatus: lastContainerStatus,
      };

    }

    if (attempt < config.containerPollMaxAttempts) {

      await sleep(config.containerPollIntervalMs);

    }

  }

  return {
    ready: false,
    attempt: config.containerPollMaxAttempts,
    status: lastStatus,
    containerStatus: lastContainerStatus,
  };

}


async function publishInstagramMediaContainer(client, config, creationId) {

  try {

    const response = await client.post(`/${config.igUserId}/media_publish`, null, {
      params: {
        creation_id: creationId,
        access_token: config.accessToken,
      },
    });

    return response?.data?.id;

  } catch (error) {

    throw attachInstagramApiFailure(error, "publish_media_container", {
      creation_id: creationId,
      ig_user_id: config.igUserId,
    });

  }

}


async function fetchInstagramMediaDetails(client, config, mediaId) {

  try {

    const response = await client.get(`/${mediaId}`, {
      params: {
        fields: "id,caption,media_product_type,permalink,timestamp",
        access_token: config.accessToken,
      },
    });

    return response?.data || null;

  } catch (error) {

    throw attachInstagramApiFailure(error, "fetch_media_details", {
      media_id: mediaId,
      ig_user_id: config.igUserId,
    });

  }

}

function buildMissingInstagramVerificationError(
  publishAccountId,
  mediaId,
  creationId,
  mediaDetails = null
) {

  return createPublishError(
    "Instagram publish completed but final media verification was incomplete",
    "MEDIA_VERIFICATION_INCOMPLETE",
    "verification_error",
    `creation_id=${creationId}; media_id=${mediaId}; permalink=${
      mediaDetails?.permalink || ""
    }; verified_media_id=${mediaDetails?.id || ""}`,
    publishAccountId
  );

}


const INSTAGRAM_PUBLISH_ACCOUNT_FIELDS = `
  id,
  platform,
  status,
  hidden,
  is_default,
  access_token,
  instagram_business_account_id,
  instagram_user_id,
  last_used_at,
  created_at,
  updated_at
`;

async function findInstagramPublishAccount(socialAccountId) {

  if (Number.isInteger(socialAccountId) && socialAccountId > 0) {

    const selected = await pool.query(
      `
        SELECT ${INSTAGRAM_PUBLISH_ACCOUNT_FIELDS}
        FROM social_accounts
        WHERE id = $1
        LIMIT 1
      `,
      [socialAccountId]
    );

    return selected.rows[0] || null;

  }

  const defaultAccount = await pool.query(
    `
      SELECT ${INSTAGRAM_PUBLISH_ACCOUNT_FIELDS}
      FROM social_accounts
      WHERE platform = 'instagram'
        AND status = 'active'
        AND hidden = false
        AND is_default = true
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
      LIMIT 1
    `
  );

  if (defaultAccount.rowCount > 0) {

    return defaultAccount.rows[0];

  }

  const latestActiveVisible = await pool.query(
    `
      SELECT ${INSTAGRAM_PUBLISH_ACCOUNT_FIELDS}
      FROM social_accounts
      WHERE platform = 'instagram'
        AND status = 'active'
        AND hidden = false
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
      LIMIT 1
    `
  );

  return latestActiveVisible.rows[0] || null;

}

function validateInstagramPublishAccount(account, requestedSocialAccountId = null) {

  const responseSocialAccountId = account?.id ?? requestedSocialAccountId ?? null;

  if (!account) {

    return createPublishError(
      "No Instagram social account is available for publishing",
      "SOCIAL_ACCOUNT_NOT_FOUND",
      "account_error",
      undefined,
      responseSocialAccountId
    );

  }

  if (String(account.platform || "").trim().toLowerCase() !== "instagram") {

    return createPublishError(
      "Selected social account platform must be instagram",
      "INVALID_PLATFORM",
      "account_error",
      undefined,
      responseSocialAccountId
    );

  }

  if (String(account.status || "").trim().toLowerCase() !== "active") {

    return createPublishError(
      "Instagram social account is not active",
      "ACCOUNT_INACTIVE",
      "account_error",
      undefined,
      responseSocialAccountId
    );

  }

  if (account.hidden === true) {

    return createPublishError(
      "Instagram social account is hidden",
      "ACCOUNT_HIDDEN",
      "account_error",
      undefined,
      responseSocialAccountId
    );

  }

  if (!String(account.access_token || "").trim()) {

    return createPublishError(
      "Instagram social account is missing access_token",
      "ACCESS_TOKEN_MISSING",
      "account_error",
      undefined,
      responseSocialAccountId
    );

  }

  const instagramBusinessAccountId = String(
    account.instagram_business_account_id || ""
  ).trim();
  const instagramUserId = String(account.instagram_user_id || "").trim();

  if (!instagramBusinessAccountId && !instagramUserId) {

    return createPublishError(
      "Instagram social account is missing instagram_business_account_id or instagram_user_id",
      "INSTAGRAM_ACCOUNT_ID_MISSING",
      "account_error",
      undefined,
      responseSocialAccountId
    );

  }

  return null;

}

async function markInstagramPublishAccountUsed(socialAccountId) {

  if (!Number.isInteger(socialAccountId) || socialAccountId <= 0) {

    return;

  }

  try {

    await pool.query(
      `
        UPDATE social_accounts
        SET last_used_at = NOW()
        WHERE id = $1
      `,
      [socialAccountId]
    );

  } catch (error) {

    console.warn(
      `[instagram] published with social account ${socialAccountId} but failed to update last_used_at: ${
        error?.message || error
      }`
    );

  }

}


async function publishToInstagram(payload) {

  const config = getInstagramPublishConfig();
  const requestedSocialAccountId =
    Number.isInteger(payload?.social_account_id) && payload.social_account_id > 0
      ? payload.social_account_id
      : null;
  const account = await findInstagramPublishAccount(requestedSocialAccountId);
  const accountError = validateInstagramPublishAccount(account, requestedSocialAccountId);

  if (accountError) {

    return accountError;

  }

  const publishAccountId = account.id;
  const instagramBusinessAccountId = String(
    account.instagram_business_account_id || ""
  ).trim();
  const instagramUserId = String(account.instagram_user_id || "").trim();
  // Legacy INSTAGRAM_* env credentials are intentionally ignored here.
  // The selected social_accounts row is the source of truth for publish auth.
  const publishConfig = {
    ...config,
    accessToken: String(account.access_token || "").trim(),
    instagramBusinessAccountId,
    instagramUserId,
    igUserId: instagramBusinessAccountId || instagramUserId,
  };

  const client = createInstagramClient(publishConfig);

  try {

    const creationId = await createInstagramMediaContainer(client, publishConfig, payload);

    if (!creationId) {

      return createPublishError(
        "Meta Graph API did not return a media container id",
        "MISSING_CREATION_ID",
        "meta_api_error",
        undefined,
        publishAccountId
      );

    }

    const containerReadiness = await waitForInstagramMediaContainerReady(
      client,
      publishConfig,
      creationId
    );

    if (!containerReadiness.ready) {

      return createPublishError(
        "Instagram media container was not ready before publish timeout",
        "MEDIA_CONTAINER_NOT_READY",
        "publish_timeout",
        `creation_id=${creationId}; last_status=${
          containerReadiness.status || "UNKNOWN"
        }; attempts=${containerReadiness.attempt}`,
        publishAccountId
      );

    }

    const mediaId = await publishInstagramMediaContainer(client, publishConfig, creationId);

    if (!mediaId) {

      return createPublishError(
        "Meta Graph API did not return a published media id",
        "MISSING_MEDIA_ID",
        "meta_api_error",
        undefined,
        publishAccountId
      );

    }

    let mediaDetails = null;

    try {

      mediaDetails = await fetchInstagramMediaDetails(client, publishConfig, mediaId);

    } catch (detailError) {

      console.warn("[instagram] failed to fetch published media details", {
        product_id: payload.product_id,
        social_account_id: publishAccountId,
        media_id: mediaId,
        provider_failure: extractInstagramApiFailure(detailError, "fetch_media_details", {
          media_id: mediaId,
          ig_user_id: publishConfig.igUserId,
        }),
      });

      const publishError = createPublishError(
        "Instagram media publish could not be verified",
        "MEDIA_VERIFICATION_FAILED",
        "verification_error",
        `creation_id=${creationId}; media_id=${mediaId}`,
        publishAccountId
      );
      publishError.error.provider_failure = extractInstagramApiFailure(
        detailError,
        "fetch_media_details",
        {
          media_id: mediaId,
          ig_user_id: publishConfig.igUserId,
        }
      );
      return publishError;

    }

    if (
      !mediaDetails ||
      String(mediaDetails.id || "").trim() !== String(mediaId).trim() ||
      !String(mediaDetails.permalink || "").trim()
    ) {

      return buildMissingInstagramVerificationError(
        publishAccountId,
        mediaId,
        creationId,
        mediaDetails
      );

    }

    await markInstagramPublishAccountUsed(publishAccountId);

    return {
      success: true,
      platform: "instagram",
      social_account_id: publishAccountId,
      publish_result: {
        external_id: mediaId,
        published_at: mediaDetails?.timestamp || new Date().toISOString(),
        url: mediaDetails?.permalink || payload.normalized_image_url,
        caption: mediaDetails?.caption || payload.caption,
      },
      error: null,
    };

  } catch (error) {

    console.warn("[instagram] publish failed", {
      product_id: payload.product_id,
      social_account_id: publishAccountId,
      provider_failure: extractInstagramApiFailure(error, "publish_to_instagram", {
        ig_user_id: publishConfig.igUserId,
        image_url: payload.normalized_image_url,
        caption_length: payload.caption ? payload.caption.length : 0,
        caption_preview: payload.caption ? payload.caption.slice(0, 280) : "",
      }),
    });
    return normalizeMetaError(error, publishAccountId);

  }

}


app.post("/publish", workerAuth, async (req, res) => {

  try {

    const { errors, value } = validatePublishBody(req.body || {});
    const requestedSocialAccountId =
      Number.isInteger(value?.social_account_id) && value.social_account_id > 0
        ? value.social_account_id
        : null;

    if (errors.length > 0) {

      return res.json(
        createPublishError(
          "Validation failed",
          "VALIDATION_ERROR",
          "validation_error",
          errors,
          requestedSocialAccountId
        )
      );

    }

    return res.json(await publishToInstagram(value));

  } catch (e) {

    console.error(`[instagram] unexpected publish error: ${e.message}`);
    return res.json(
      createPublishError(
        e.message || "Server error",
        "SERVER_ERROR",
        "server_error"
      )
    );

  }

});



// ========================

// Worker Claim

// ========================



app.post("/worker/claim", workerAuth, async (req, res) => {

  const workerId = req.body?.worker_id || "worker-1";

  const leaseSeconds = Math.min(Math.max(Number(req.body?.lease_seconds || 60), 10), 600);

  const result = await claimQueueItem({ workerId, leaseSeconds });
  return res.status(result.httpStatus).json(result.body);

});


function computeBackoffSeconds(attempt) {
  const base = getInternalRepostRuntimeConfig().scheduler.retrySeconds;
  return Math.min(
    DEFAULT_INTERNAL_REPOST_MAX_RETRY_SECONDS,
    base * Math.pow(2, Math.max(0, attempt - 1))
  );
}

function createInternalRepostTrace(action, queueItemId = null) {
  return {
    traceId: crypto.randomUUID(),
    action,
    queue_item_id: Number.isFinite(queueItemId) ? queueItemId : null,
    processing_engine: "internal",
  };
}

function logInternalRepost(level, event, context = {}) {
  const logger =
    level === "error" ? console.error : level === "warn" ? console.warn : console.info;

  recordInternalRepostActivity(level, event, context);
  logger(`[internal-repost] ${event}`, context);
}

async function claimQueueItem({ workerId, leaseSeconds }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    console.log("[queue-debug] claim attempt", {
      worker_id: workerId,
      lease_seconds: leaseSeconds,
    });

    const pick = await client.query(
      `
      SELECT pq.id
      FROM products_queue pq
      WHERE pq.status IN ('scheduled', 'published', 'failed')
        AND pq.next_run_at <= NOW()
        AND COALESCE(pq.publish_count, 0) < COALESCE(pq.max_republish, $1)
        AND COALESCE(pq.attempts, 0) < COALESCE(pq.max_attempts, 8)
        AND EXISTS (
          SELECT 1
          FROM social_accounts sa
          WHERE sa.platform = 'instagram'
            AND sa.status = 'active'
            AND sa.hidden = false
        )
      ORDER BY pq.next_run_at ASC, pq.id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
      `,
      [DEFAULT_QUEUE_MAX_REPUBLISH]
    );

    if (pick.rowCount === 0) {
      console.log("[queue-debug] repost eligibility decision", {
        claimed: false,
        reason: "no_due_repost_item",
      });

      await client.query("COMMIT");
      return {
        httpStatus: 200,
        body: { item: null },
      };
    }

    const id = pick.rows[0].id;
    const claimedRow = await client.query(
      `
      SELECT *
      FROM products_queue
      WHERE id=$1
      FOR UPDATE
      `,
      [id]
    );
    const queueItem = claimedRow.rows[0];
    const imageSelection = await getNextValidQueueImageSelection(queueItem);

    const upd = await client.query(
      `
      UPDATE products_queue
      SET status='processing',
          last_image=$5,
          image_rotation_state=$6::jsonb,
          processing_started_at=NOW(),
          locked_by=$2,
          locked_at=NOW(),
          lease_expires_at=NOW() + ($3 || ' seconds')::interval,
          updated_at=NOW()
      WHERE id=$1
        AND status IN ('scheduled', 'published', 'failed')
        AND next_run_at <= NOW()
        AND COALESCE(publish_count, 0) < COALESCE(max_republish, $4)
      RETURNING *
      `,
      [
        id,
        workerId,
        String(leaseSeconds),
        DEFAULT_QUEUE_MAX_REPUBLISH,
        imageSelection.selectedImage,
        imageSelection.nextRotationState
          ? JSON.stringify(imageSelection.nextRotationState)
          : null,
      ]
    );

    if (upd.rowCount > 0) {
      const claimedItem = upd.rows[0];
      const currentPublishCount = Number.isInteger(claimedItem.publish_count)
        ? claimedItem.publish_count
        : 0;
      const effectiveMaxRepublish = resolveQueueMaxRepublish(claimedItem.max_republish);

      console.log("[queue-debug] repost eligibility decision", {
        id: claimedItem.id,
        product_id: claimedItem.product_id,
        status: claimedItem.status,
        publish_count: currentPublishCount,
        max_republish: claimedItem.max_republish,
        resolved_max_republish: effectiveMaxRepublish,
        next_run_at: claimedItem.next_run_at,
        eligible_for_repost: currentPublishCount < effectiveMaxRepublish,
      });

      console.log("[queue-debug] claim success", {
        worker_id: workerId,
        id: claimedItem.id,
        product_id: claimedItem.product_id,
        previous_image: imageSelection.previousImage || null,
        next_candidate_image: imageSelection.nextCandidateImage || null,
        skipped_invalid_images: imageSelection.skippedInvalidImages,
        selected_image: claimedItem.last_image || null,
        updated_rotation_state: claimedItem.image_rotation_state || null,
        image_rotation_source: imageSelection.source,
        image_count: imageSelection.imageCount,
        locked_by: claimedItem.locked_by,
        lease_expires_at: claimedItem.lease_expires_at,
      });
    }

    await client.query("COMMIT");
    return {
      httpStatus: 200,
      body: { item: upd.rows[0] },
    };
  } catch (e) {
    await client.query("ROLLBACK");
    return {
      httpStatus: 500,
      body: { error: "Server error", message: e.message },
    };
  } finally {
    client.release();
  }
}

async function failClaimedQueueItem({ id, workerId, error, errorCode }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const row = await client.query("SELECT * FROM products_queue WHERE id=$1 FOR UPDATE", [id]);
    if (row.rowCount === 0) {
      await client.query("ROLLBACK");
      return {
        httpStatus: 404,
        body: { error: "Not found" },
      };
    }

    const item = row.rows[0];
    if (item.status !== "processing" || item.locked_by !== workerId) {
      await client.query("ROLLBACK");
      return {
        httpStatus: 409,
        body: { error: "Not owned by this worker" },
      };
    }

    const nextAttempts = (item.attempts || 0) + 1;
    const maxAttempts = item.max_attempts ?? 8;
    const delay = computeBackoffSeconds(nextAttempts);
    const formattedError = errorCode ? `[${errorCode}] ${error}` : error;
    const isTerminal = nextAttempts >= maxAttempts;

    const upd = await client.query(
      `
      UPDATE products_queue
      SET status='failed',
          attempts=$2,
          publish_fail_count=COALESCE(publish_fail_count, 0) + 1,
          last_error=$3,
          last_error_at=NOW(),
          next_run_at=CASE
            WHEN $5 THEN NOW() + INTERVAL '100 years'
            ELSE NOW() + ($4 || ' seconds')::interval
          END,
          lease_expires_at=NULL,
          locked_by=NULL,
          locked_at=NULL,
          processing_started_at=NULL,
          updated_at=NOW(),
          finished_at=NOW()
      WHERE id=$1
      RETURNING *
      `,
      [id, nextAttempts, formattedError, String(delay), isTerminal]
    );

    await client.query("COMMIT");

    if (isTerminal) {
      return {
        httpStatus: 200,
        body: { item: upd.rows[0], terminal: true },
      };
    }

    return {
      httpStatus: 200,
      body: { item: upd.rows[0], retry_in_seconds: delay },
    };
  } catch (e) {
    await client.query("ROLLBACK");
    return {
      httpStatus: 500,
      body: { error: "Server error", message: e.message },
    };
  } finally {
    client.release();
  }
}

async function succeedClaimedQueueItem({ id, workerId }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const row = await client.query(
      `
      SELECT *
      FROM products_queue
      WHERE id=$1 AND status='processing' AND locked_by=$2
      FOR UPDATE
      `,
      [id, workerId]
    );

    if (row.rowCount === 0) {
      await client.query("ROLLBACK");
      return {
        httpStatus: 409,
        body: { error: "Not owned by this worker" },
      };
    }

    const item = row.rows[0];
    const publishDecision = getQueuePublishDecision(item, { requireDueNow: false });
    const currentPublishCount = Number.isInteger(item.publish_count) ? item.publish_count : 0;
    const nextPublishCount = currentPublishCount + 1;
    const effectiveMaxRepublish = resolveQueueMaxRepublish(item.max_republish);
    const shouldContinueReposting = nextPublishCount < effectiveMaxRepublish;
    const nextStatus = shouldContinueReposting ? "scheduled" : "published";
    const nextRunAt = shouldContinueReposting
      ? computeNextQueueRunAt(nextPublishCount)
      : new Date().toISOString();

    console.log("[queue-debug] publish success", {
      source: "worker_succeed",
      id: item.id,
      product_id: item.product_id,
      publish_count_before: publishDecision.currentPublishCount,
      publish_count_after: nextPublishCount,
      max_republish: effectiveMaxRepublish,
      worker_id: workerId,
    });

    if (nextPublishCount === 1) {
      console.log("[queue-debug] item completion after first publish", {
        id: item.id,
        product_id: item.product_id,
        publish_count: nextPublishCount,
        max_republish: effectiveMaxRepublish,
        status_after_publish: nextStatus,
      });
    }

    console.log("[queue-debug] next_run_at reassignment", {
      id: item.id,
      product_id: item.product_id,
      publish_count: nextPublishCount,
      max_republish: effectiveMaxRepublish,
      next_run_at: nextRunAt,
      repost_continues: shouldContinueReposting,
    });

    const updated = await client.query(
      `
      UPDATE products_queue
      SET publish_count=$2,
          attempts=0,
          status=$3,
          next_run_at=$4,
          last_error=NULL,
          last_error_at=NULL,
          processing_started_at=NULL,
          locked_by=NULL,
          locked_at=NULL,
          lease_expires_at=NULL,
          updated_at=NOW(),
          finished_at=NOW()
      WHERE id=$1
      RETURNING *
      `,
      [id, nextPublishCount, nextStatus, nextRunAt]
    );

    await client.query("COMMIT");
    return {
      httpStatus: 200,
      body: { item: updated.rows[0] },
    };
  } catch (e) {
    await client.query("ROLLBACK");
    return {
      httpStatus: 500,
      body: { error: "Server error", message: e.message },
    };
  } finally {
    client.release();
  }
}

async function requeueClaimedQueueItem({ id, workerId }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const row = await client.query(
      `
      SELECT *
      FROM products_queue
      WHERE id=$1 AND status='processing' AND locked_by=$2
      FOR UPDATE
      `,
      [id, workerId]
    );

    if (row.rowCount === 0) {
      await client.query("ROLLBACK");
      return {
        httpStatus: 409,
        body: { error: "Not owned by this worker" },
      };
    }

    const updated = await client.query(
      `
      UPDATE products_queue
      SET status='scheduled',
          processing_started_at=NULL,
          locked_by=NULL,
          locked_at=NULL,
          lease_expires_at=NULL,
          next_run_at=NOW(),
          updated_at=NOW()
      WHERE id=$1
      RETURNING *
      `,
      [id]
    );

    await client.query("COMMIT");
    return {
      httpStatus: 200,
      body: { item: updated.rows[0] },
    };
  } catch (e) {
    await client.query("ROLLBACK");
    return {
      httpStatus: 500,
      body: { error: "Server error", message: e.message },
    };
  } finally {
    client.release();
  }
}

function buildInternalRepostResponse({
  ok,
  status,
  action,
  trace,
  message,
  item = null,
  extra = {},
}) {
  return {
    ok,
    status,
    processing_engine: trace.processing_engine,
    action,
    trace_id: trace.traceId,
    queue_item_id: item?.id ?? trace.queue_item_id ?? null,
    item,
    message,
    ...extra,
  };
}

function buildDashboardFeatureFlagsResponse() {
  const runtimeConfig = getInternalRepostRuntimeConfig();
  const configuredFeatureFlags = runtimeConfig.configuredFeatureFlags;
  const effectiveFeatureFlags = runtimeConfig.featureFlags;
  return INTERNAL_REPOST_FEATURE_FLAG_DEFINITIONS.reduce((accumulator, definition) => {
    const configuredValue = configuredFeatureFlags[definition.responseKey] === true;
    const effectiveValue = effectiveFeatureFlags[definition.responseKey] === true;
    accumulator[definition.responseKey] = {
      value: configuredValue,
      effective_value: effectiveValue,
      editable: definition.editable === true,
      env_key: definition.envKey,
      overridden_by_mode: configuredValue !== effectiveValue,
    };
    return accumulator;
  }, {});
}

function buildDashboardConfigResponse() {
  const runtimeConfig = getInternalRepostRuntimeConfig();
  const modeSummary = buildInternalRepostModeSummary(runtimeConfig);
  return {
    ok: true,
    config_save_supported: true,
    config_save_path: "/internal/repost/config/save",
    config_editable: true,
    openrouter_api_key_masked: runtimeConfig.openRouter.apiKeyMasked,
    openrouter_api_key_configured: runtimeConfig.openRouter.apiKeyConfigured,
    openrouter_model: runtimeConfig.openRouter.model,
    openrouter_base_url: runtimeConfig.openRouter.baseUrl,
    openrouter_timeout_ms: runtimeConfig.openRouter.timeoutMs,
    worker_id: runtimeConfig.scheduler.workerId,
    lease_seconds: runtimeConfig.scheduler.leaseSeconds,
    retry_seconds: runtimeConfig.scheduler.retrySeconds,
    scheduler_interval_ms: runtimeConfig.scheduler.intervalMs,
    max_runs_per_tick: runtimeConfig.scheduler.maxRunsPerTick,
    concurrency: runtimeConfig.scheduler.concurrency,
    dry_run: runtimeConfig.scheduler.dryRun,
    internal_repost_mode: runtimeConfig.operatingMode,
    internal_repost_mode_source: runtimeConfig.operatingModeSource,
    primary_publishing_path: modeSummary.primaryPublishingPath,
    status_badges: modeSummary.statusBadges,
    status_message: modeSummary.statusMessage,
    feature_flags: buildDashboardFeatureFlagsResponse(),
    manual_actions: buildInternalRepostManualActions(runtimeConfig.configuredFeatureFlags),
    settings_loaded_at: runtimeSettingsState.lastLoadedAt,
    capabilities: {
      config_read: true,
      config_save: true,
    },
    actions: {
      config_save: {
        supported: true,
        path: "/internal/repost/config/save",
        method: "POST",
      },
    },
  };
}

function validateDashboardConfigPayload(body) {
  const source = normalizeRequestBodyObject(body);
  const errors = [];
  const updates = [];

  const pushSettingUpdate = (settingKey, value, isSecret = false) => {
    updates.push({ settingKey, value, isSecret });
  };

  if (hasOwn(source, "openrouter_api_key")) {
    const apiKey = source.openrouter_api_key == null ? "" : String(source.openrouter_api_key).trim();
    pushSettingUpdate(`${OPENROUTER_SETTINGS_PREFIX}api_key`, apiKey, true);
  }

  if (hasOwn(source, "openrouter_model")) {
    const model = normalizeOptionalString(source.openrouter_model);
    if (!model) {
      errors.push("openrouter_model must be a non-empty string");
    } else {
      pushSettingUpdate(`${OPENROUTER_SETTINGS_PREFIX}model`, model);
    }
  }

  if (hasOwn(source, "openrouter_base_url")) {
    const baseUrl = normalizeOptionalString(source.openrouter_base_url);
    if (!baseUrl) {
      errors.push("openrouter_base_url must be a non-empty string");
    } else {
      try {
        const parsed = new URL(baseUrl);
        if (!["http:", "https:"].includes(parsed.protocol)) {
          errors.push("openrouter_base_url must use http or https");
        } else {
          pushSettingUpdate(`${OPENROUTER_SETTINGS_PREFIX}base_url`, parsed.toString().replace(/\/+$/, ""));
        }
      } catch {
        errors.push("openrouter_base_url must be a valid URL");
      }
    }
  }

  const numericFieldConfigs = [
    {
      bodyKey: "openrouter_timeout_ms",
      settingKey: `${OPENROUTER_SETTINGS_PREFIX}timeout_ms`,
      min: 1000,
      max: 300000,
    },
    {
      bodyKey: "lease_seconds",
      settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}lease_seconds`,
      min: 10,
      max: 600,
    },
    {
      bodyKey: "retry_seconds",
      settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}retry_seconds`,
      min: 5,
      max: DEFAULT_INTERNAL_REPOST_MAX_RETRY_SECONDS,
    },
    {
      bodyKey: "scheduler_interval_ms",
      settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}scheduler_interval_ms`,
      min: 1000,
      max: 3600000,
    },
    {
      bodyKey: "max_runs_per_tick",
      settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}max_runs_per_tick`,
      min: 1,
      max: 100,
    },
    {
      bodyKey: "concurrency",
      settingKey: `${INTERNAL_REPOST_SETTINGS_PREFIX}concurrency`,
      min: 1,
      max: 25,
    },
  ];

  for (const fieldConfig of numericFieldConfigs) {
    if (!hasOwn(source, fieldConfig.bodyKey)) {
      continue;
    }

    const parsed = normalizeOptionalNonNegativeInteger(source[fieldConfig.bodyKey]);
    if (parsed == null || parsed < fieldConfig.min || parsed > fieldConfig.max) {
      errors.push(
        `${fieldConfig.bodyKey} must be an integer between ${fieldConfig.min} and ${fieldConfig.max}`
      );
      continue;
    }

    pushSettingUpdate(fieldConfig.settingKey, parsed);
  }

  if (hasOwn(source, "worker_id")) {
    const workerId = normalizeOptionalString(source.worker_id);
    if (!workerId) {
      errors.push("worker_id must be a non-empty string");
    } else if (workerId.length > 120) {
      errors.push("worker_id must be 120 characters or fewer");
    } else {
      pushSettingUpdate(`${INTERNAL_REPOST_SETTINGS_PREFIX}worker_id`, workerId);
    }
  }

  if (hasOwn(source, "dry_run")) {
    const dryRun = normalizeOptionalBoolean(source.dry_run);
    if (dryRun == null) {
      errors.push("dry_run must be a boolean");
    } else {
      pushSettingUpdate(`${INTERNAL_REPOST_SETTINGS_PREFIX}dry_run`, dryRun);
    }
  }

  if (hasOwn(source, "internal_repost_mode")) {
    const operatingMode = normalizeInternalRepostMode(source.internal_repost_mode);
    if (!operatingMode) {
      errors.push("internal_repost_mode must be 'shadow' or 'internal_primary'");
    } else {
      pushSettingUpdate(INTERNAL_REPOST_MODE_SETTING_KEY, operatingMode);
    }
  }

  for (const definition of INTERNAL_REPOST_FEATURE_FLAG_DEFINITIONS) {
    if (!hasOwn(source, definition.responseKey)) {
      continue;
    }

    if (definition.editable !== true) {
      errors.push(`${definition.responseKey} is read-only`);
      continue;
    }

    const parsed = normalizeOptionalBoolean(source[definition.responseKey]);
    if (parsed == null) {
      errors.push(`${definition.responseKey} must be a boolean`);
      continue;
    }

    pushSettingUpdate(definition.settingKey, parsed);
  }

  return { errors, updates };
}

async function saveDashboardConfig({ updates, updatedBy }) {
  for (const update of updates) {
    await upsertRuntimeSetting({
      settingKey: update.settingKey,
      value: update.value,
      isSecret: update.isSecret === true,
      updatedBy,
    });
  }

  await loadRuntimeSettingsFromDb();
}

async function syncInternalRepostSchedulerLifecycle() {
  internalRepostScheduler.stop();
  internalRepostScheduler.start();
}

async function buildDashboardHealth() {
  const runtimeConfig = getInternalRepostRuntimeConfig();
  const schedulerStatus = internalRepostScheduler.getStatus();
  const modeSummary = buildInternalRepostModeSummary(runtimeConfig, schedulerStatus);
  const issues = [];
  const notices = [];
  let publishAvailable = false;

  try {
    const account = await findInstagramPublishAccount(null);
    publishAvailable = Boolean(account);
    if (!publishAvailable) {
      issues.push("No active visible Instagram social account is available for publishing");
    }
  } catch (error) {
    issues.push(error?.message || "Failed to inspect publish availability");
  }

  if (!runtimeConfig.featureFlags.use_internal_rewrite) {
    issues.push("Internal rewrite feature flag is disabled");
  } else if (!runtimeConfig.openRouter.apiKeyConfigured) {
    issues.push("OpenRouter API key is missing");
  }

  if (!runtimeConfig.featureFlags.use_internal_publish) {
    issues.push("Internal publish feature flag is disabled");
  }

  if (modeSummary.internalRepostMode === INTERNAL_REPOST_MODE_SHADOW) {
    notices.push("Workflow / n8n is primary; internal automatic repost processing is intentionally disabled");
  } else if (!runtimeConfig.featureFlags.use_internal_scheduler) {
    issues.push("Internal scheduler feature flag is disabled");
  }

  return {
    backend_reachable: true,
    scheduler_endpoint_healthy: schedulerStatus.ok === true,
    rewrite_available:
      runtimeConfig.featureFlags.use_internal_rewrite &&
      runtimeConfig.openRouter.apiKeyConfigured,
    publish_available:
      runtimeConfig.featureFlags.use_internal_publish && publishAvailable,
    openrouter_configured: runtimeConfig.openRouter.apiKeyConfigured,
    primary_publishing_path: modeSummary.primaryPublishingPath,
    internal_repost_mode: modeSummary.internalRepostMode,
    auto_processing_enabled: modeSummary.autoProcessingEnabled,
    config_issues_summary: issues,
    config_notices_summary: notices,
  };
}

async function claimSpecificQueueItemForManualAction({ id, workerId, leaseSeconds }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const row = await client.query(
      `
      SELECT *
      FROM products_queue
      WHERE id = $1
      FOR UPDATE
      `,
      [id]
    );

    if (row.rowCount === 0) {
      await client.query("ROLLBACK");
      return {
        httpStatus: 404,
        body: { error: "Queue item not found" },
      };
    }

    const item = row.rows[0];
    if (item.status === "processing") {
      if (item.locked_by !== workerId) {
        await client.query("ROLLBACK");
        return {
          httpStatus: 409,
          body: { error: "Queue item is already owned by another worker" },
        };
      }

      await client.query("COMMIT");
      return {
        httpStatus: 200,
        body: { item, claimed: false, already_owned: true },
      };
    }

    const updatableStatuses = ["scheduled", "published", "failed"];
    if (!updatableStatuses.includes(String(item.status || "").toLowerCase())) {
      await client.query("ROLLBACK");
      return {
        httpStatus: 409,
        body: { error: "Queue item is not in a manually actionable state" },
      };
    }

    const imageSelection = await getNextValidQueueImageSelection(item);
    const updated = await client.query(
      `
      UPDATE products_queue
      SET status='processing',
          last_image=$3,
          image_rotation_state=$4::jsonb,
          processing_started_at=NOW(),
          locked_by=$2,
          locked_at=NOW(),
          lease_expires_at=NOW() + ($5 || ' seconds')::interval,
          updated_at=NOW()
      WHERE id=$1
      RETURNING *
      `,
      [
        id,
        workerId,
        imageSelection.selectedImage,
        imageSelection.nextRotationState
          ? JSON.stringify(imageSelection.nextRotationState)
          : null,
        String(leaseSeconds),
      ]
    );

    console.log("[queue-debug] manual claim success", {
      worker_id: workerId,
      id: updated.rows[0]?.id ?? item.id,
      product_id: updated.rows[0]?.product_id ?? item.product_id,
      previous_image: imageSelection.previousImage || null,
      next_candidate_image: imageSelection.nextCandidateImage || null,
      skipped_invalid_images: imageSelection.skippedInvalidImages,
      selected_image: updated.rows[0]?.last_image || null,
      updated_rotation_state: updated.rows[0]?.image_rotation_state || null,
      image_rotation_source: imageSelection.source,
      image_count: imageSelection.imageCount,
      locked_by: updated.rows[0]?.locked_by || workerId,
      lease_expires_at: updated.rows[0]?.lease_expires_at || null,
    });

    await client.query("COMMIT");
    return {
      httpStatus: 200,
      body: { item: updated.rows[0], claimed: true, already_owned: false },
    };
  } catch (error) {
    await client.query("ROLLBACK");
    return {
      httpStatus: 500,
      body: { error: "Server error", message: error.message },
    };
  } finally {
    client.release();
  }
}

function getManualActionRequestContext(body) {
  const source = normalizeRequestBodyObject(body);
  const queueItemId = normalizeOptionalInteger(
    source.queue_item_id ?? source.id ?? source.product_queue_id
  );
  const productId = normalizeOptionalInteger(source.product_id);
  const runtimeConfig = getInternalRepostRuntimeConfig();
  const workerId =
    normalizeOptionalString(source.worker_id) ||
    `${runtimeConfig.scheduler.workerId}-dashboard`;
  const leaseSeconds = hasOwn(source, "lease_seconds")
    ? normalizeOptionalNonNegativeInteger(source.lease_seconds)
    : runtimeConfig.scheduler.leaseSeconds;

  return {
    queueItemId,
    productId,
    workerId,
    leaseSeconds:
      leaseSeconds != null && leaseSeconds >= 10 && leaseSeconds <= 600
        ? leaseSeconds
        : runtimeConfig.scheduler.leaseSeconds,
    dryRunRequested: normalizeOptionalBoolean(source.dry_run) === true,
    rewrittenFields:
      source.rewritten_fields && typeof source.rewritten_fields === "object"
        ? source.rewritten_fields
        : null,
  };
}

async function findManualActionQueueItemsByProductId(productId) {
  if (!Number.isInteger(productId) || productId <= 0) {
    return [];
  }

  const result = await pool.query(
    `
    SELECT *
    FROM products_queue
    WHERE product_id = $1
      AND status IN ('processing', 'scheduled', 'published', 'failed')
    ORDER BY
      CASE WHEN status = 'processing' THEN 0 ELSE 1 END,
      next_run_at ASC NULLS LAST,
      updated_at DESC,
      id DESC
    LIMIT 3
    `,
    [productId]
  );

  return result.rows;
}

function getManualActionStatusPriority(status) {
  switch (String(status || "").trim().toLowerCase()) {
    case "scheduled":
      return 0;
    case "failed":
      return 1;
    case "published":
      return 2;
    case "processing":
      return 3;
    default:
      return 9;
  }
}

function getManualActionReasonMessage(candidate, actionLabel) {
  switch (candidate.reason) {
    case "eligible":
      return `Queue item ${candidate.queue_item_id} is eligible for ${actionLabel}`;
    case "owned_by_this_worker":
      return `Queue item ${candidate.queue_item_id} is already claimed by this worker and eligible for ${actionLabel}`;
    case "already_processing":
      return `Queue item ${candidate.queue_item_id} is already being processed by ${candidate.locked_by || "another worker"}`;
    case "attempt_limit_reached":
      return `Queue item ${candidate.queue_item_id} reached max_attempts (${candidate.max_attempts})`;
    case "lifecycle_exhausted":
      return `Queue item ${candidate.queue_item_id} reached max_republish (${candidate.max_republish})`;
    case "status_not_publishable":
      return `Queue item ${candidate.queue_item_id} status ${candidate.status || "unknown"} is not eligible for ${actionLabel}`;
    default:
      return `Queue item ${candidate.queue_item_id} is not eligible for ${actionLabel}`;
  }
}

function summarizeManualActionCandidate(item, workerId, action) {
  const publishDecision = getQueuePublishDecision(item, { requireDueNow: false });
  const attempts = Number.isInteger(item?.attempts) ? item.attempts : 0;
  const maxAttempts =
    Number.isInteger(item?.max_attempts) && item.max_attempts > 0
      ? item.max_attempts
      : 8;
  const ownedByThisWorker =
    publishDecision.currentStatus === "processing" && item?.locked_by === workerId;
  const blockedByOtherWorker =
    publishDecision.currentStatus === "processing" && !ownedByThisWorker;
  const hasAttemptsRemaining = attempts < maxAttempts;
  const eligible =
    (publishDecision.eligible && hasAttemptsRemaining) || ownedByThisWorker;
  const reason = eligible
    ? ownedByThisWorker
      ? "owned_by_this_worker"
      : "eligible"
    : blockedByOtherWorker
      ? "already_processing"
      : !hasAttemptsRemaining
        ? "attempt_limit_reached"
        : publishDecision.reason;
  const parsedNextRunAt =
    item?.next_run_at == null ? null : new Date(item.next_run_at);
  const hasValidParsedNextRunAt =
    parsedNextRunAt != null && !Number.isNaN(parsedNextRunAt.getTime());
  const nextRunAt =
    publishDecision.nextRunAt ||
    (hasValidParsedNextRunAt ? parsedNextRunAt.toISOString() : null);
  const actionLabel = String(action || "manual repost test").replaceAll("_", " ");

  return {
    queue_item_id: item.id,
    product_id: item.product_id ?? null,
    status: publishDecision.currentStatus || null,
    eligible,
    reason,
    reason_message: getManualActionReasonMessage(
      {
        queue_item_id: item.id,
        status: publishDecision.currentStatus || null,
        reason,
        locked_by: item?.locked_by || null,
        max_attempts: maxAttempts,
        max_republish: publishDecision.effectiveMaxRepublish,
      },
      actionLabel
    ),
    next_run_at: nextRunAt,
    publish_count: publishDecision.currentPublishCount,
    max_republish: publishDecision.effectiveMaxRepublish,
    attempts,
    max_attempts: maxAttempts,
    locked_by: item?.locked_by || null,
    selected_by_rank: {
      status_priority: getManualActionStatusPriority(publishDecision.currentStatus),
      next_run_at: nextRunAt,
      updated_at: item?.updated_at || null,
      id: item.id,
    },
  };
}

// Prefer a valid scheduled repost item first, then the nearest next_run_at,
// then the most recently updated row, with id as a final deterministic tie-breaker.
function compareManualActionCandidates(a, b) {
  const statusPriorityDiff =
    getManualActionStatusPriority(a.status) - getManualActionStatusPriority(b.status);
  if (statusPriorityDiff !== 0) {
    return statusPriorityDiff;
  }

  const aNextRun = a.next_run_at ? new Date(a.next_run_at).getTime() : 0;
  const bNextRun = b.next_run_at ? new Date(b.next_run_at).getTime() : 0;
  if (aNextRun !== bNextRun) {
    return aNextRun - bNextRun;
  }

  const aUpdatedAt = a.selected_by_rank.updated_at
    ? new Date(a.selected_by_rank.updated_at).getTime()
    : 0;
  const bUpdatedAt = b.selected_by_rank.updated_at
    ? new Date(b.selected_by_rank.updated_at).getTime()
    : 0;
  if (aUpdatedAt !== bUpdatedAt) {
    return bUpdatedAt - aUpdatedAt;
  }

  return b.queue_item_id - a.queue_item_id;
}

function buildManualActionResolverResponse({
  ok,
  queueItemId = null,
  productId = null,
  resolvedBy = null,
  queueItem = null,
  errorResponse = null,
}) {
  return {
    ok,
    queueItemId,
    productId,
    resolvedBy,
    queueItem,
    errorResponse,
  };
}

async function resolveManualActionQueueItem(requestContext) {
  const actionLabel = String(requestContext.action || "manual repost test").replaceAll(
    "_",
    " "
  );

  const buildSingleQueueItemResponse = (item, resolvedBy) => {
    const candidate = summarizeManualActionCandidate(
      item,
      requestContext.workerId,
      requestContext.action
    );
    if (!candidate.eligible) {
      return buildManualActionResolverResponse({
        ok: false,
        errorResponse: {
          httpStatus: 409,
          body: {
            ok: false,
            status: "not_eligible",
            queue_item_id: item.id,
            product_id: item.product_id ?? null,
            resolved_by: resolvedBy,
            message: candidate.reason_message,
            candidate,
          },
        },
      });
    }

    return buildManualActionResolverResponse({
      ok: true,
      queueItemId: item.id,
      productId: item.product_id ?? null,
      resolvedBy,
      queueItem: item,
    });
  };

  const buildProductResolutionResponse = (productId, queueItems, resolvedBy) => {
    if (queueItems.length === 0) {
      return buildManualActionResolverResponse({
        ok: false,
        errorResponse: {
          httpStatus: 404,
          body: {
            ok: false,
            status: "no_candidate",
            product_id: productId,
            message: `Product ${productId} has no active repost queue item for ${actionLabel}`,
          },
        },
      });
    }

    const candidates = queueItems
      .map((item) =>
        summarizeManualActionCandidate(item, requestContext.workerId, requestContext.action)
      )
      .sort(compareManualActionCandidates);
    const eligibleCandidates = candidates.filter((candidate) => candidate.eligible);

    if (eligibleCandidates.length === 0) {
      return buildManualActionResolverResponse({
        ok: false,
        errorResponse: {
          httpStatus: 409,
          body: {
            ok: false,
            status: "not_eligible",
            product_id: productId,
            candidate_queue_item_ids: candidates.map((candidate) => candidate.queue_item_id),
            message: `Product ${productId} has queue items, but none are repost-eligible for ${actionLabel}`,
            candidates,
          },
        },
      });
    }

    const selectedCandidate = eligibleCandidates[0];
    const selectedItem = queueItems.find((item) => item.id === selectedCandidate.queue_item_id);

    return buildManualActionResolverResponse({
      ok: true,
      queueItemId: selectedCandidate.queue_item_id,
      productId,
      resolvedBy,
      queueItem: selectedItem || null,
    });
  };

  if (Number.isInteger(requestContext.queueItemId) && requestContext.queueItemId > 0) {
    const queueRow = await pool.query(
      `
      SELECT *
      FROM products_queue
      WHERE id = $1
      LIMIT 1
      `,
      [requestContext.queueItemId]
    );

    if (queueRow.rowCount > 0) {
      return buildSingleQueueItemResponse(queueRow.rows[0], "queue_item_id");
    }

    const productMatches = await findManualActionQueueItemsByProductId(
      requestContext.queueItemId
    );
    const productFallback = buildProductResolutionResponse(
      requestContext.queueItemId,
      productMatches,
      "product_id"
    );

    if (productFallback.ok) {
      return productFallback;
    }

    if (productMatches.length > 0) {
      return {
        ...productFallback,
        errorResponse: {
          httpStatus: productFallback.errorResponse.httpStatus,
          body: {
            ...productFallback.errorResponse.body,
            queue_item_id: null,
            message:
              productFallback.errorResponse.body.message ||
              `Provided id ${requestContext.queueItemId} resolved as product_id, but no valid queue item could be used.`,
          },
        },
      };
    }

    return buildManualActionResolverResponse({
      ok: false,
      errorResponse: {
        httpStatus: 404,
        body: {
          ok: false,
          status: "not_found",
          queue_item_id: requestContext.queueItemId,
          message: `Queue item ${requestContext.queueItemId} was not found`,
        },
      },
    });
  }

  if (Number.isInteger(requestContext.productId) && requestContext.productId > 0) {
    const productMatches = await findManualActionQueueItemsByProductId(
      requestContext.productId
    );
    return buildProductResolutionResponse(
      requestContext.productId,
      productMatches,
      "product_id"
    );
  }

  return buildManualActionResolverResponse({
    ok: false,
    errorResponse: {
      httpStatus: 400,
      body: {
        ok: false,
        status: "invalid_request",
        error: "queue_item_id must be a positive integer",
        message: "Provide queue_item_id or product_id.",
      },
    },
  });
}

function buildCurrentQueueImageSelection(item) {
  const claimedLastImage = normalizeOptionalString(item?.last_image);
  const normalizedImages = normalizeQueueImages(item?.images);

  if (claimedLastImage) {
    return {
      selectedImage: claimedLastImage,
      imageCount: Array.isArray(normalizedImages) ? normalizedImages.length : 0,
      source: "claimed_last_image",
    };
  }

  const nextSelection = getNextQueueImageSelection(item);
  return {
    selectedImage: nextSelection.selectedImage,
    imageCount: nextSelection.imageCount,
    source: nextSelection.source,
  };
}

function buildPreparedRepostData(item, socialAccount = null) {
  const serializedItem = serializeQueueItemForApi(item);
  const title = normalizeOptionalString(item?.last_title);
  const description = normalizeOptionalString(item?.last_description);
  const caption = normalizeOptionalString(item?.last_caption);
  const hashtags = normalizeQueueHashtags(item?.last_hashtags);
  const sourceUrl = normalizeOptionalString(item?.url);
  const images = normalizeQueueImages(item?.images);
  const selection = buildCurrentQueueImageSelection(item);
  const normalizedPublishInput = {
    platform: "instagram",
    product_id: item?.product_id,
    image_url: selection.selectedImage,
    title,
    description,
    caption,
    hashtags,
  };
  if (Number.isInteger(socialAccount?.id) && socialAccount.id > 0) {
    normalizedPublishInput.social_account_id = socialAccount.id;
  }
  const normalizedPublish = validatePublishBody(normalizedPublishInput);

  return {
    queue_item_id: item.id,
    product_id: item.product_id || null,
    source_url: sourceUrl,
    url: sourceUrl,
    title,
    description,
    caption,
    normalized_caption: normalizedPublish.value?.caption || null,
    normalized_caption_length: normalizedPublish.value?.caption_length ?? 0,
    normalized_caption_was_truncated: normalizedPublish.value?.caption_was_truncated === true,
    hashtags,
    images,
    selected_image: selection.selectedImage || null,
    selected_image_source: selection.source,
    image_count: selection.imageCount,
    last_image: normalizeOptionalString(item?.last_image),
    stored_last_image: serializedItem?.stored_last_image ?? null,
    preview_image: serializedItem?.preview_image ?? null,
    publish_count: Number.isInteger(item?.publish_count) ? item.publish_count : 0,
    max_republish: resolveQueueMaxRepublish(item?.max_republish),
    status: item?.status || null,
    platform: "instagram",
    social_account_id: socialAccount?.id ?? null,
    social_account: socialAccount
      ? {
          id: socialAccount.id,
          platform: socialAccount.platform,
          status: socialAccount.status,
          hidden: socialAccount.hidden,
          is_default: socialAccount.is_default,
        }
      : null,
    workflow_source: item?.workflow_source ?? null,
    locked_by: item?.locked_by || null,
    lease_expires_at: item?.lease_expires_at || null,
    queue_item: serializedItem,
  };
}

function normalizeRewriteValue(value) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizeRewriteFields(parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return null;
  }

  const title = normalizeRewriteValue(parsed.title ?? parsed.headline ?? parsed.name);
  const description = normalizeRewriteValue(
    parsed.description ?? parsed.body ?? parsed.summary
  );
  const caption = normalizeRewriteValue(parsed.caption ?? parsed.post_caption ?? parsed.copy);
  const hashtags = normalizeQueueHashtags(
    parsed.hashtags ?? parsed.hash_tags ?? parsed.tags ?? parsed.tagline
  );

  if (!title && !description && !caption && !hashtags) {
    return null;
  }

  return {
    title,
    description,
    caption,
    hashtags,
  };
}

function extractJsonCandidate(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    return null;
  }

  const fencedMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch?.[1]) {
    return fencedMatch[1].trim();
  }

  const firstBrace = trimmed.indexOf("{");
  const lastBrace = trimmed.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    return trimmed.slice(firstBrace, lastBrace + 1);
  }

  return trimmed;
}

function parseRewriteResponse(rawText) {
  const candidate = extractJsonCandidate(rawText);
  if (!candidate) {
    return {
      ok: false,
      parsed: null,
      error: "Rewrite response was empty",
    };
  }

  try {
    const parsedJson = JSON.parse(candidate);
    const normalized = normalizeRewriteFields(parsedJson);
    if (!normalized) {
      return {
        ok: false,
        parsed: null,
        error: "Rewrite response JSON did not contain usable fields",
      };
    }

    return {
      ok: true,
      parsed: normalized,
      error: null,
    };
  } catch {
    return {
      ok: false,
      parsed: null,
      error: "Rewrite response could not be parsed as JSON",
    };
  }
}

function getOpenRouterConfig() {
  const runtimeConfig = getInternalRepostRuntimeConfig().openRouter;

  return {
    apiKey: runtimeConfig.apiKey,
    model: runtimeConfig.model,
    baseUrl: runtimeConfig.baseUrl,
    timeoutMs: runtimeConfig.timeoutMs,
  };
}

function buildRewritePrompt(prepared) {
  return [
    "You rewrite repost content for an Instagram product post.",
    "Return JSON only with keys: title, description, caption, hashtags.",
    "Keep the output concise, marketable, and safe for repost publishing.",
    "Preserve the product meaning and do not invent facts.",
    "",
    "Current context:",
    JSON.stringify(
      {
        product_id: prepared.product_id,
        source_url: prepared.source_url,
        title: prepared.title,
        description: prepared.description,
        caption: prepared.caption,
        hashtags: prepared.hashtags,
      },
      null,
      2
    ),
  ].join("\n");
}

async function callOpenRouterRewrite(prepared) {
  const config = getOpenRouterConfig();
  if (!config.apiKey) {
    return {
      ok: false,
      status: "provider_failed",
      provider: "openrouter",
      model: config.model,
      rawText: "",
      timingMs: 0,
      message: "OPENROUTER_API_KEY is not configured",
    };
  }

  const startedAt = Date.now();
  try {
    const response = await axios.post(
      `${config.baseUrl}/chat/completions`,
      {
        model: config.model,
        messages: [
          {
            role: "system",
            content:
              "You are a structured content rewriter. Output valid JSON only.",
          },
          {
            role: "user",
            content: buildRewritePrompt(prepared),
          },
        ],
        temperature: 0.7,
        response_format: { type: "json_object" },
      },
      {
        timeout: config.timeoutMs,
        headers: {
          Authorization: `Bearer ${config.apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );

    const rawText = String(
      response?.data?.choices?.[0]?.message?.content ??
        response?.data?.choices?.[0]?.text ??
        ""
    ).trim();

    return {
      ok: true,
      status: "rewritten",
      provider: "openrouter",
      model: config.model,
      rawText,
      timingMs: Date.now() - startedAt,
      message: "Rewrite provider call succeeded",
    };
  } catch (error) {
    return {
      ok: false,
      status: "provider_failed",
      provider: "openrouter",
      model: config.model,
      rawText: "",
      timingMs: Date.now() - startedAt,
      message:
        error?.response?.data?.error?.message ||
        error?.message ||
        "OpenRouter rewrite request failed",
    };
  }
}

async function prepareClaimedRepostContext({ id, workerId }) {
  try {
    const row = await pool.query(
      `
      SELECT *
      FROM products_queue
      WHERE id=$1 AND status='processing' AND locked_by=$2
      LIMIT 1
      `,
      [id, workerId]
    );

    if (row.rowCount === 0) {
      return {
        httpStatus: 409,
        status: "invalid",
        message: "Queue item is not owned by this worker",
        prepared: null,
      };
    }

    const item = row.rows[0];
    const socialAccount = await findInstagramPublishAccount(null);
    const prepared = buildPreparedRepostData(item, socialAccount);

    if (!prepared.product_id) {
      return {
        httpStatus: 422,
        status: "invalid",
        message: "Queue item is missing product_id",
        prepared,
      };
    }

    if (!prepared.selected_image) {
      return {
        httpStatus: 422,
        status: "invalid",
        message: "Queue item is missing a selected image",
        prepared,
      };
    }

    return {
      httpStatus: 200,
      status: "prepared",
      message: "Repost input prepared",
      prepared,
    };
  } catch (e) {
    return {
      httpStatus: 500,
      status: "error",
      message: e.message || "Failed to prepare repost input",
      prepared: null,
    };
  }
}

function buildValidatedPublishPayload(prepared) {
  const payloadCandidate = {
    platform: prepared.platform || "instagram",
    product_id: prepared.product_id,
    title: prepared.title,
    description: prepared.description,
    caption: prepared.caption,
    hashtags: prepared.hashtags,
    image: prepared.selected_image,
    image_url: prepared.selected_image,
  };
  if (Number.isInteger(prepared.social_account_id) && prepared.social_account_id > 0) {
    payloadCandidate.social_account_id = prepared.social_account_id;
  }
  const { errors, value } = validatePublishBody(payloadCandidate);

  if (errors.length > 0) {
    return {
      httpStatus: 422,
      status: "invalid",
      message: "Publish payload validation failed",
      prepared,
      payload: {
        ...payloadCandidate,
        queue_item_id: prepared.queue_item_id,
        source_url: prepared.source_url,
      },
      errors,
    };
  }

  return {
    httpStatus: 200,
    status: "built",
    message: "Publish payload built",
    prepared,
    payload: {
      ...value,
      queue_item_id: prepared.queue_item_id,
      source_url: prepared.source_url,
      selected_image: prepared.selected_image,
      publish_count: prepared.publish_count,
      max_republish: prepared.max_republish,
      caption_length: value.caption_length ?? 0,
      caption_was_truncated: value.caption_was_truncated === true,
      caption_components: value.caption_components ?? null,
    },
  };
}

async function buildInternalPublishPayloadFromQueueItem({ id, workerId }) {
  const preparedResult = await prepareClaimedRepostContext({ id, workerId });
  if (preparedResult.httpStatus >= 400) {
    return {
      ...preparedResult,
      payload: null,
    };
  }

  return buildValidatedPublishPayload(preparedResult.prepared);
}

function applyRewriteFieldsToPrepared(prepared, rewriteFields = null) {
  const normalizedRewrite = normalizeRewriteFields(rewriteFields);
  if (!normalizedRewrite) {
    return {
      prepared,
      applied: false,
      rewriteFields: null,
    };
  }

  return {
    applied: true,
    rewriteFields: normalizedRewrite,
    prepared: {
      ...prepared,
      title: normalizedRewrite.title ?? prepared.title,
      description: normalizedRewrite.description ?? prepared.description,
      caption: normalizedRewrite.caption ?? prepared.caption,
      hashtags: normalizedRewrite.hashtags ?? prepared.hashtags,
    },
  };
}

async function rewriteClaimedRepostContext({ id, workerId }) {
  const preparedResult = await prepareClaimedRepostContext({ id, workerId });
  if (preparedResult.httpStatus >= 400) {
    return {
      ...preparedResult,
      rewriteResult: null,
    };
  }

  const providerResult = await callOpenRouterRewrite(preparedResult.prepared);
  if (!providerResult.ok) {
    return {
      httpStatus: 200,
      status: "provider_failed",
      message: providerResult.message,
      prepared: preparedResult.prepared,
      rewriteResult: {
        raw_text: providerResult.rawText,
        parsed: null,
        provider: providerResult.provider,
        model: providerResult.model,
        timing_ms: providerResult.timingMs,
      },
    };
  }

  const parsedResult = parseRewriteResponse(providerResult.rawText);
  if (!parsedResult.ok) {
    return {
      httpStatus: 200,
      status: "parse_failed",
      message: parsedResult.error,
      prepared: preparedResult.prepared,
      rewriteResult: {
        raw_text: providerResult.rawText,
        parsed: null,
        provider: providerResult.provider,
        model: providerResult.model,
        timing_ms: providerResult.timingMs,
      },
    };
  }

  return {
    httpStatus: 200,
    status: "rewritten",
    message: "Rewrite completed successfully",
    prepared: preparedResult.prepared,
    rewriteResult: {
      raw_text: providerResult.rawText,
      parsed: parsedResult.parsed,
      provider: providerResult.provider,
      model: providerResult.model,
      timing_ms: providerResult.timingMs,
    },
  };
}

function extractPublishFailureDetails(publishResponse) {
  const message =
    publishResponse?.error?.message ||
    publishResponse?.message ||
    "Internal repost publish failed";
  const code =
    publishResponse?.error?.code == null ? null : String(publishResponse.error.code);

  return {
    message,
    code,
  };
}

function getInternalSchedulerConfig() {
  return {
    ...getInternalRepostRuntimeConfig().scheduler,
  };
}

async function applyInternalPublishStatusUpdate({ id, workerId, publishResponse, dryRun = false }) {
  if (dryRun || !getInternalRepostFeatureFlags().use_internal_publish_mark_status) {
    return null;
  }

  if (normalizeConfirmedInstagramPublishResponse(publishResponse)) {
    return succeedClaimedQueueItem({ id, workerId });
  }

  const failure = extractPublishFailureDetails(publishResponse);
  return failClaimedQueueItem({
    id,
    workerId,
    error: failure.message,
    errorCode: failure.code,
  });
}

async function executeInternalPublishForQueueItem({
  id,
  workerId,
  dryRun = false,
  rewrittenFields = null,
}) {
  let rewriteResult = null;
  const rewriteRequested = getInternalRepostFeatureFlags().use_internal_rewrite_for_publish === true;
  let payloadResult = await buildInternalPublishPayloadFromQueueItem({ id, workerId });

  if (payloadResult.httpStatus >= 400) {
    return {
      ...payloadResult,
      publishResponse: null,
      statusUpdate: null,
      rewriteResult: null,
    };
  }

  const explicitRewrite = applyRewriteFieldsToPrepared(payloadResult.prepared, rewrittenFields);
  if (explicitRewrite.applied) {
    const explicitPayloadCandidate = {
      platform: explicitRewrite.prepared.platform || "instagram",
      product_id: explicitRewrite.prepared.product_id,
      title: explicitRewrite.prepared.title,
      description: explicitRewrite.prepared.description,
      caption: explicitRewrite.prepared.caption,
      hashtags: explicitRewrite.prepared.hashtags,
      image: explicitRewrite.prepared.selected_image,
      image_url: explicitRewrite.prepared.selected_image,
    };
    if (
      Number.isInteger(explicitRewrite.prepared.social_account_id) &&
      explicitRewrite.prepared.social_account_id > 0
    ) {
      explicitPayloadCandidate.social_account_id = explicitRewrite.prepared.social_account_id;
    }

    const { errors, value } = validatePublishBody(explicitPayloadCandidate);
    if (errors.length === 0) {
      payloadResult = buildValidatedPublishPayload(explicitRewrite.prepared);
    }
  } else if (rewriteRequested) {
    const rewriteAttempt = await rewriteClaimedRepostContext({ id, workerId });
    rewriteResult = rewriteAttempt.rewriteResult;

    if (rewriteAttempt.status === "rewritten" && rewriteAttempt.rewriteResult?.parsed) {
      const rewrittenPrepared = applyRewriteFieldsToPrepared(
        payloadResult.prepared,
        rewriteAttempt.rewriteResult.parsed
      );
      const payloadCandidate = {
        platform: rewrittenPrepared.prepared.platform || "instagram",
        product_id: rewrittenPrepared.prepared.product_id,
        title: rewrittenPrepared.prepared.title,
        description: rewrittenPrepared.prepared.description,
        caption: rewrittenPrepared.prepared.caption,
        hashtags: rewrittenPrepared.prepared.hashtags,
        image: rewrittenPrepared.prepared.selected_image,
        image_url: rewrittenPrepared.prepared.selected_image,
      };
      if (
        Number.isInteger(rewrittenPrepared.prepared.social_account_id) &&
        rewrittenPrepared.prepared.social_account_id > 0
      ) {
        payloadCandidate.social_account_id = rewrittenPrepared.prepared.social_account_id;
      }
      const { errors, value } = validatePublishBody(payloadCandidate);
      if (errors.length === 0) {
        payloadResult = buildValidatedPublishPayload(rewrittenPrepared.prepared);
      }
    } else {
      payloadResult = {
        ...payloadResult,
        rewrite_fallback_used: true,
      };
    }
  }

  if (dryRun === true) {
    return {
      httpStatus: 200,
      status: "published",
      message: "Internal repost publish dry run completed",
      prepared: payloadResult.prepared,
      payload: payloadResult.payload,
      publishResponse: {
        success: true,
        dry_run: true,
        platform: payloadResult.payload?.platform || "instagram",
        social_account_id: payloadResult.payload?.social_account_id ?? null,
        publish_result: null,
        error: null,
      },
      statusUpdate: null,
      rewriteResult,
    };
  }

  const imageValidation = await validateInstagramPublishImageSelection(
    payloadResult.prepared,
    payloadResult.payload
  );
  if (!imageValidation.ok) {
    return {
      httpStatus: 200,
      status: "publish_failed",
      message: imageValidation.error?.error?.message || imageValidation.error?.message,
      prepared: payloadResult.prepared,
      payload: payloadResult.payload,
      publishResponse: imageValidation.error,
      statusUpdate: null,
      rewriteResult,
    };
  }

  const publishResponse = await publishToInstagram(payloadResult.payload);
  const wasSuccessful = publishResponse?.success === true;

  return {
    httpStatus: wasSuccessful ? 200 : 200,
    status: wasSuccessful ? "published" : "publish_failed",
    message: wasSuccessful
      ? "Internal repost publish succeeded"
      : extractPublishFailureDetails(publishResponse).message,
    prepared: payloadResult.prepared,
    payload: payloadResult.payload,
    publishResponse,
    statusUpdate: null,
    rewriteResult,
  };
}

async function runSingleInternalRepostCycle({
  trigger = "manual",
  dryRun = false,
  workerId = DEFAULT_INTERNAL_SCHEDULER_WORKER_ID,
  config = getInternalSchedulerConfig(),
}) {
  const trace = createInternalRepostTrace("full_repost_cycle");
  const steps = {
    claim: null,
    prepare: null,
    rewrite: null,
    build_payload: null,
    publish: null,
    status_update: null,
  };

  logInternalRepost("info", "full_repost_cycle_started", {
    ...trace,
    action: "full_repost_cycle",
    trigger,
    dry_run: dryRun,
    worker_id: workerId,
  });

  if (!config.schedulerEnabled || !config.fullFlowEnabled) {
    return {
      ok: false,
      status: "disabled",
      processing_engine: "internal",
      action: "full_repost_cycle",
      trace_id: trace.traceId,
      queue_item_id: null,
      steps,
      message: !config.schedulerEnabled
        ? "Internal scheduler is disabled"
        : "Internal full repost flow is disabled",
    };
  }

  const claimResult = await claimQueueItem({
    workerId,
    leaseSeconds: config.leaseSeconds,
  });
  const claimedItem = claimResult.body?.item ?? null;
  steps.claim = {
    ok: claimResult.httpStatus < 400,
    status: claimedItem ? "claimed" : claimResult.httpStatus >= 400 ? "error" : "no_item",
    item: claimedItem,
    message:
      claimResult.body?.message ||
      claimResult.body?.error ||
      (claimedItem ? "Queue item claimed" : "No eligible repost queue item is available"),
  };

  if (claimResult.httpStatus >= 400) {
    return {
      ok: false,
      status: "error",
      processing_engine: "internal",
      action: "full_repost_cycle",
      trace_id: trace.traceId,
      queue_item_id: null,
      steps,
      message: steps.claim.message,
    };
  }

  if (!claimedItem) {
    logInternalRepost("info", "claim_no_item", {
      ...trace,
      action: "full_repost_cycle",
      status: "no_item",
    });

    return {
      ok: true,
      status: "no_item",
      processing_engine: "internal",
      action: "full_repost_cycle",
      trace_id: trace.traceId,
      queue_item_id: null,
      steps,
      message: "No eligible repost queue item is available",
    };
  }

  trace.queue_item_id = claimedItem.id;

  const prepareResult = await prepareClaimedRepostContext({ id: claimedItem.id, workerId });
  steps.prepare = {
    ok: prepareResult.httpStatus < 400,
    status: prepareResult.status,
    prepared: prepareResult.prepared,
    message: prepareResult.message,
  };

  if (prepareResult.httpStatus >= 400) {
    return {
      ok: false,
      status: prepareResult.status,
      processing_engine: "internal",
      action: "full_repost_cycle",
      trace_id: trace.traceId,
      queue_item_id: claimedItem.id,
      steps,
      message: prepareResult.message,
    };
  }

  let rewrittenFields = null;
  if (getInternalRepostFeatureFlags().use_internal_rewrite) {
    const rewriteResult = await rewriteClaimedRepostContext({ id: claimedItem.id, workerId });
    steps.rewrite = {
      ok: rewriteResult.status === "rewritten",
      status: rewriteResult.status,
      rewrite_result: rewriteResult.rewriteResult,
      message: rewriteResult.message,
    };
    if (rewriteResult.status === "rewritten") {
      rewrittenFields = rewriteResult.rewriteResult?.parsed ?? null;
    } else {
      logInternalRepost("info", "rewrite_fallback_to_stored_fields", {
        ...trace,
        action: "full_repost_cycle",
        status: rewriteResult.status,
        queue_item_id: claimedItem.id,
        provider: rewriteResult.rewriteResult?.provider ?? "openrouter",
        model: rewriteResult.rewriteResult?.model ?? getOpenRouterConfig().model,
      });
    }
  }

  let buildPrepared = prepareResult.prepared;
  if (rewrittenFields && getInternalRepostFeatureFlags().use_internal_rewrite_for_publish) {
    buildPrepared = applyRewriteFieldsToPrepared(prepareResult.prepared, rewrittenFields).prepared;
  }
  const buildPayloadResult = buildValidatedPublishPayload(buildPrepared);
  steps.build_payload = {
    ok: buildPayloadResult.httpStatus < 400,
    status: buildPayloadResult.status,
    payload: buildPayloadResult.payload,
    message: buildPayloadResult.message,
  };

  if (buildPayloadResult.httpStatus >= 400) {
    return {
      ok: false,
      status: buildPayloadResult.status,
      processing_engine: "internal",
      action: "full_repost_cycle",
      trace_id: trace.traceId,
      queue_item_id: claimedItem.id,
      steps,
      message: buildPayloadResult.message,
    };
  }

  const publishResult = await executeInternalPublishForQueueItem({
    id: claimedItem.id,
    workerId,
    dryRun,
    rewrittenFields: getInternalRepostFeatureFlags().use_internal_rewrite_for_publish
      ? rewrittenFields
      : null,
  });
  steps.publish = {
    ok: publishResult.publishResponse?.success === true,
    status: publishResult.status,
    publish_result: publishResult.publishResponse,
    payload: publishResult.payload,
    message: publishResult.message,
  };

  const statusUpdate = await applyInternalPublishStatusUpdate({
    id: claimedItem.id,
    workerId,
    publishResponse: publishResult.publishResponse,
    dryRun,
  });
  steps.status_update = statusUpdate
    ? {
        ok: statusUpdate.httpStatus < 400,
        status: statusUpdate.httpStatus < 400 ? "updated" : "error",
        result: statusUpdate.body,
      }
    : null;

  const finalOk = publishResult.publishResponse?.success === true;
  const finalStatus = finalOk ? "published" : "publish_failed";

  logInternalRepost(finalOk ? "info" : "warn", finalOk ? "full_flow_success" : "full_flow_failure", {
    ...trace,
    action: "full_repost_cycle",
    status: finalStatus,
    dry_run: dryRun,
    queue_item_id: claimedItem.id,
  });

  return {
    ok: finalOk,
    status: finalStatus,
    processing_engine: "internal",
    action: "full_repost_cycle",
    trace_id: trace.traceId,
    queue_item_id: claimedItem.id,
    steps,
    message: publishResult.message,
  };
}

const internalRepostFlowRunner = new InternalRepostFlowRunner({
  getConfig: getInternalSchedulerConfig,
  executeCycle: runSingleInternalRepostCycle,
});

const internalRepostScheduler = new InternalRepostScheduler({
  getConfig: getInternalSchedulerConfig,
  runner: internalRepostFlowRunner,
  log: (level, event, context) => logInternalRepost(level, event, context),
});



app.post("/worker/fail/:id", workerAuth, async (req, res) => {

  const id = Number(req.params.id);

  const workerId = req.body?.worker_id || "worker-1";

  const err = (req.body?.error || "unknown").toString().slice(0, 5000);

  const errorCode = req.body?.error_code == null ? null : String(req.body.error_code).slice(0, 255);

  if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid id" });
  const result = await failClaimedQueueItem({
    id,
    workerId,
    error: err,
    errorCode,
  });

  return res.status(result.httpStatus).json(result.body);

});




app.post("/worker/succeed/:id", workerAuth, async (req, res) => {

  const id = Number(req.params.id);

  const workerId = req.body?.worker_id || "worker-1";

  if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid id" });
  const publishResponse = req.body?.publish_response || req.body?.publishResponse || null;
  const confirmedPublish = normalizeConfirmedInstagramPublishResponse(publishResponse);
  if (!confirmedPublish) {
    return res.status(422).json({
      error: "Confirmed Instagram publish verification is required before marking success",
      required_fields: ["publish_response.publish_result.external_id", "publish_response.publish_result.url", "publish_response.publish_result.published_at"],
    });
  }
  const result = await succeedClaimedQueueItem({ id, workerId });
  return res.status(result.httpStatus).json(result.body);

});

app.post("/internal/repost/run-once", workerAuth, async (req, res) => {
  const trace = createInternalRepostTrace("run_once");
  const workerId = req.body?.worker_id || "internal-repost";
  const leaseSeconds = Math.min(Math.max(Number(req.body?.lease_seconds || 60), 10), 600);

  logInternalRepost("info", "claim_requested", {
    ...trace,
    worker_id: workerId,
    lease_seconds: leaseSeconds,
  });

  if (!getInternalRepostFeatureFlags().use_internal_claim) {
    logInternalRepost("warn", "feature_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json(
      buildInternalRepostResponse({
        ok: false,
        status: "disabled",
        action: "run_once",
        trace,
        message: "Internal repost claim is disabled",
      })
    );
  }

  const result = await claimQueueItem({ workerId, leaseSeconds });
  const item = result.body?.item ?? null;

  if (result.httpStatus >= 400) {
    logInternalRepost("error", "unexpected_error", {
      ...trace,
      status: "error",
      message: result.body?.message || result.body?.error || "Internal repost claim failed",
    });

    return res.status(result.httpStatus).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "run_once",
        trace,
        item,
        message: result.body?.message || result.body?.error || "Internal repost claim failed",
      })
    );
  }

  if (!item) {
    logInternalRepost("info", "no_item_available", {
      ...trace,
      status: "no_item",
    });

    return res.json(
      buildInternalRepostResponse({
        ok: true,
        status: "no_item",
        action: "run_once",
        trace,
        message: "No eligible repost queue item is available",
      })
    );
  }

  trace.queue_item_id = item.id;
  logInternalRepost("info", "item_claimed", {
    ...trace,
    status: "claimed",
    worker_id: workerId,
  });

  return res.json(
    buildInternalRepostResponse({
      ok: true,
      status: "claimed",
      action: "run_once",
      trace,
      item,
      message: "Repost queue item claimed",
    })
  );
});

app.post("/internal/repost/:id/done", workerAuth, repostNumericIdRouteGuard("done"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("done", id);
  const workerId = req.body?.worker_id || "internal-repost";

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
    });

    return res.status(400).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "done",
        trace,
        message: "Invalid id",
      })
    );
  }

  if (!getInternalRepostFeatureFlags().use_internal_status_updates) {
    logInternalRepost("warn", "feature_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json(
      buildInternalRepostResponse({
        ok: false,
        status: "disabled",
        action: "done",
        trace,
        message: "Internal repost status updates are disabled",
      })
    );
  }

  const result = await succeedClaimedQueueItem({ id, workerId });
  const item = result.body?.item ?? null;

  if (result.httpStatus >= 400) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: result.body?.message || result.body?.error || "Internal repost done failed",
    });

    return res.status(result.httpStatus).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "done",
        trace,
        item,
        message: result.body?.message || result.body?.error || "Internal repost done failed",
      })
    );
  }

  logInternalRepost("info", "done", {
    ...trace,
    queue_item_id: item?.id ?? id,
    status: "done",
    worker_id: workerId,
  });

  return res.json(
    buildInternalRepostResponse({
      ok: true,
      status: "done",
      action: "done",
      trace,
      item,
      message: "Repost queue item marked done",
    })
  );
});

app.post("/internal/repost/:id/fail", workerAuth, repostNumericIdRouteGuard("fail"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("fail", id);
  const workerId = req.body?.worker_id || "internal-repost";
  const err = (req.body?.error || "unknown").toString().slice(0, 5000);
  const errorCode = req.body?.error_code == null ? null : String(req.body.error_code).slice(0, 255);

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
    });

    return res.status(400).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "fail",
        trace,
        message: "Invalid id",
      })
    );
  }

  if (!getInternalRepostFeatureFlags().use_internal_status_updates) {
    logInternalRepost("warn", "feature_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json(
      buildInternalRepostResponse({
        ok: false,
        status: "disabled",
        action: "fail",
        trace,
        message: "Internal repost status updates are disabled",
      })
    );
  }

  const result = await failClaimedQueueItem({
    id,
    workerId,
    error: err,
    errorCode,
  });
  const item = result.body?.item ?? null;

  if (result.httpStatus >= 400) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: result.body?.message || result.body?.error || "Internal repost fail failed",
    });

    return res.status(result.httpStatus).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "fail",
        trace,
        item,
        message: result.body?.message || result.body?.error || "Internal repost fail failed",
      })
    );
  }

  logInternalRepost("info", "fail", {
    ...trace,
    queue_item_id: item?.id ?? id,
    status: "failed",
    worker_id: workerId,
    terminal: Boolean(result.body?.terminal),
    retry_in_seconds: result.body?.retry_in_seconds ?? null,
  });

  return res.json(
    buildInternalRepostResponse({
      ok: true,
      status: "failed",
      action: "fail",
      trace,
      item,
      message: "Repost queue item marked failed",
      extra: {
        terminal: Boolean(result.body?.terminal),
        retry_in_seconds: result.body?.retry_in_seconds ?? null,
      },
    })
  );
});

app.post("/internal/repost/:id/requeue", workerAuth, repostNumericIdRouteGuard("requeue"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("requeue", id);
  const workerId = req.body?.worker_id || "internal-repost";

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
    });

    return res.status(400).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "requeue",
        trace,
        message: "Invalid id",
      })
    );
  }

  if (!getInternalRepostFeatureFlags().use_internal_status_updates) {
    logInternalRepost("warn", "feature_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json(
      buildInternalRepostResponse({
        ok: false,
        status: "disabled",
        action: "requeue",
        trace,
        message: "Internal repost status updates are disabled",
      })
    );
  }

  const result = await requeueClaimedQueueItem({ id, workerId });
  const item = result.body?.item ?? null;

  if (result.httpStatus >= 400) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: result.body?.message || result.body?.error || "Internal repost requeue failed",
    });

    return res.status(result.httpStatus).json(
      buildInternalRepostResponse({
        ok: false,
        status: "error",
        action: "requeue",
        trace,
        item,
        message: result.body?.message || result.body?.error || "Internal repost requeue failed",
      })
    );
  }

  logInternalRepost("info", "requeue", {
    ...trace,
    queue_item_id: item?.id ?? id,
    status: "requeued",
    worker_id: workerId,
  });

  return res.json(
    buildInternalRepostResponse({
      ok: true,
      status: "requeued",
      action: "requeue",
      trace,
      item,
      message: "Repost queue item requeued",
    })
  );
});

app.post("/internal/repost/:id/prepare", workerAuth, repostNumericIdRouteGuard("prepare"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("prepare", id);
  const workerId = req.body?.worker_id || "internal-repost";

  logInternalRepost("info", "prepare_requested", {
    ...trace,
    worker_id: workerId,
  });

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
    });

    return res.status(400).json({
      ok: false,
      status: "error",
      processing_engine: "internal",
      action: "prepare",
      trace_id: trace.traceId,
      queue_item_id: null,
      prepared: null,
      message: "Invalid id",
    });
  }

  if (!getInternalRepostFeatureFlags().use_internal_prepare_repost) {
    logInternalRepost("warn", "prepare_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json({
      ok: false,
      status: "disabled",
      processing_engine: "internal",
      action: "prepare",
      trace_id: trace.traceId,
      queue_item_id: id,
      prepared: null,
      message: "Internal repost prepare is disabled",
    });
  }

  const result = await prepareClaimedRepostContext({ id, workerId });

  if (result.httpStatus >= 400) {
    logInternalRepost(result.httpStatus >= 500 ? "error" : "warn", "invalid_queue_item", {
      ...trace,
      status: result.status,
      message: result.message,
      queue_item_id: result.prepared?.queue_item_id ?? id,
    });

    return res.status(result.httpStatus).json({
      ok: false,
      status: result.status,
      processing_engine: "internal",
      action: "prepare",
      trace_id: trace.traceId,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      prepared: result.prepared,
      message: result.message,
    });
  }

  if (!result.prepared?.selected_image) {
    logInternalRepost("warn", "missing_image_data", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
    });
  }

  logInternalRepost("info", "prepare_success", {
    ...trace,
    status: result.status,
    queue_item_id: result.prepared?.queue_item_id ?? id,
    selected_image_source: result.prepared?.selected_image_source ?? null,
  });

  return res.json({
    ok: true,
    status: "prepared",
    processing_engine: "internal",
    action: "prepare",
    trace_id: trace.traceId,
    queue_item_id: result.prepared?.queue_item_id ?? id,
    prepared: result.prepared,
    message: result.message,
  });
});

app.post("/internal/repost/:id/build-payload", workerAuth, repostNumericIdRouteGuard("build_payload"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("build_payload", id);
  const workerId = req.body?.worker_id || "internal-repost";

  logInternalRepost("info", "build_payload_requested", {
    ...trace,
    worker_id: workerId,
  });

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
    });

    return res.status(400).json({
      ok: false,
      status: "error",
      processing_engine: "internal",
      action: "build_payload",
      trace_id: trace.traceId,
      queue_item_id: null,
      payload: null,
      message: "Invalid id",
    });
  }

  if (!getInternalRepostFeatureFlags().use_internal_publish_payload) {
    logInternalRepost("warn", "build_payload_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json({
      ok: false,
      status: "disabled",
      processing_engine: "internal",
      action: "build_payload",
      trace_id: trace.traceId,
      queue_item_id: id,
      payload: null,
      message: "Internal repost publish payload building is disabled",
    });
  }

  const result = await buildInternalPublishPayloadFromQueueItem({ id, workerId });

  if (result.httpStatus >= 400) {
    const eventName = result.status === "invalid" ? "invalid_queue_item" : "unexpected_error";
    logInternalRepost(result.httpStatus >= 500 ? "error" : "warn", eventName, {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      message: result.message,
      errors: result.errors ?? null,
    });

    return res.status(result.httpStatus).json({
      ok: false,
      status: result.status,
      processing_engine: "internal",
      action: "build_payload",
      trace_id: trace.traceId,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      payload: result.payload,
      message: result.message,
    });
  }

  if (!result.payload?.selected_image) {
    logInternalRepost("warn", "missing_image_data", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
    });
  }

  logInternalRepost("info", "build_payload_success", {
    ...trace,
    status: result.status,
    queue_item_id: result.prepared?.queue_item_id ?? id,
    selected_image_source: result.prepared?.selected_image_source ?? null,
    caption_length: result.payload?.caption_length ?? result.payload?.caption?.length ?? 0,
    caption_was_truncated: result.payload?.caption_was_truncated === true,
  });

  return res.json({
    ok: true,
    status: "built",
    processing_engine: "internal",
    action: "build_payload",
    trace_id: trace.traceId,
    queue_item_id: result.prepared?.queue_item_id ?? id,
    payload: result.payload,
    message: result.message,
  });
});

app.post("/internal/repost/:id/rewrite", workerAuth, repostNumericIdRouteGuard("rewrite"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("rewrite", id);
  const workerId = req.body?.worker_id || "internal-repost";

  logInternalRepost("info", "rewrite_requested", {
    ...trace,
    worker_id: workerId,
    provider: "openrouter",
    model: getOpenRouterConfig().model,
  });

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
      provider: "openrouter",
    });

    return res.status(400).json({
      ok: false,
      status: "error",
      processing_engine: "internal",
      action: "rewrite",
      trace_id: trace.traceId,
      queue_item_id: null,
      rewrite_result: null,
      message: "Invalid id",
    });
  }

  if (!getInternalRepostFeatureFlags().use_internal_rewrite) {
    logInternalRepost("warn", "rewrite_disabled", {
      ...trace,
      status: "disabled",
      provider: "openrouter",
    });

    return res.json({
      ok: false,
      status: "disabled",
      processing_engine: "internal",
      action: "rewrite",
      trace_id: trace.traceId,
      queue_item_id: id,
      rewrite_result: null,
      message: "Internal repost rewrite is disabled",
    });
  }

  logInternalRepost("info", "provider_request_started", {
    ...trace,
    provider: "openrouter",
    model: getOpenRouterConfig().model,
  });

  const result = await rewriteClaimedRepostContext({ id, workerId });

  if (result.httpStatus >= 400 || result.status === "invalid" || result.status === "error") {
    logInternalRepost(result.httpStatus >= 500 ? "error" : "warn", "validation_failure", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      message: result.message,
      provider: "openrouter",
    });

    return res.status(result.httpStatus).json({
      ok: false,
      status: result.status,
      processing_engine: "internal",
      action: "rewrite",
      trace_id: trace.traceId,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      rewrite_result: result.rewriteResult,
      message: result.message,
    });
  }

  if (result.status === "provider_failed") {
    logInternalRepost("warn", "provider_failure", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      provider: result.rewriteResult?.provider ?? "openrouter",
      model: result.rewriteResult?.model ?? getOpenRouterConfig().model,
      message: result.message,
    });
  } else if (result.status === "parse_failed") {
    logInternalRepost("warn", "parse_failure", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      provider: result.rewriteResult?.provider ?? "openrouter",
      model: result.rewriteResult?.model ?? getOpenRouterConfig().model,
      message: result.message,
    });
  } else {
    logInternalRepost("info", "provider_success", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      provider: result.rewriteResult?.provider ?? "openrouter",
      model: result.rewriteResult?.model ?? getOpenRouterConfig().model,
      timing_ms: result.rewriteResult?.timing_ms ?? null,
    });
    logInternalRepost("info", "parse_success", {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      provider: result.rewriteResult?.provider ?? "openrouter",
      model: result.rewriteResult?.model ?? getOpenRouterConfig().model,
    });
  }

  return res.json({
    ok: result.status === "rewritten",
    status: result.status,
    processing_engine: "internal",
    action: "rewrite",
    trace_id: trace.traceId,
    queue_item_id: result.prepared?.queue_item_id ?? id,
    rewrite_result: result.rewriteResult,
    message: result.message,
  });
});

app.post("/internal/repost/:id/publish", workerAuth, repostNumericIdRouteGuard("publish"), async (req, res) => {
  const id = Number(req.params.id);
  const trace = createInternalRepostTrace("publish", id);
  const workerId = req.body?.worker_id || "internal-repost";
  const dryRun = req.body?.dry_run === true;
  const rewrittenFields =
    req.body?.rewritten_fields && typeof req.body.rewritten_fields === "object"
      ? req.body.rewritten_fields
      : null;

  logInternalRepost("info", "publish_requested", {
    ...trace,
    worker_id: workerId,
    dry_run: dryRun,
    caption_length: rewrittenFields?.caption
      ? buildInstagramSafeCaption({
          caption: rewrittenFields.caption,
          title: rewrittenFields.title,
          description: rewrittenFields.description,
          hashtags: rewrittenFields.hashtags,
        }).caption_length
      : null,
  });

  if (!Number.isFinite(id)) {
    logInternalRepost("warn", "validation_failure", {
      ...trace,
      status: "error",
      message: "Invalid id",
    });

    return res.status(400).json({
      ok: false,
      status: "error",
      processing_engine: "internal",
      action: "publish",
      trace_id: trace.traceId,
      queue_item_id: null,
      publish_result: null,
      message: "Invalid id",
    });
  }

  if (!getInternalRepostFeatureFlags().use_internal_publish) {
    logInternalRepost("warn", "publish_disabled", {
      ...trace,
      status: "disabled",
    });

    return res.json({
      ok: false,
      status: "disabled",
      processing_engine: "internal",
      action: "publish",
      trace_id: trace.traceId,
      queue_item_id: id,
      publish_result: null,
      message: "Internal repost publish is disabled",
    });
  }

  const result = await executeInternalPublishForQueueItem({
    id,
    workerId,
    dryRun,
    rewrittenFields,
  });

  if (result.httpStatus >= 400 || result.status === "invalid" || result.status === "error") {
    const eventName = result.status === "invalid" ? "validation_failure" : "unexpected_error";
    logInternalRepost(result.httpStatus >= 500 ? "error" : "warn", eventName, {
      ...trace,
      status: result.status,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      message: result.message,
      errors: result.errors ?? null,
    });

    return res.status(result.httpStatus).json({
      ok: false,
      status: result.status,
      processing_engine: "internal",
      action: "publish",
      trace_id: trace.traceId,
      queue_item_id: result.prepared?.queue_item_id ?? id,
      publish_result: result.publishResponse,
      payload: result.payload,
      message: result.message,
    });
  }

  let statusUpdate = null;

  if (dryRun) {
    logInternalRepost("info", "publish_dry_run", {
      ...trace,
      status: "published",
      queue_item_id: result.prepared?.queue_item_id ?? id,
      caption_length: result.payload?.caption_length ?? result.payload?.caption?.length ?? 0,
      caption_was_truncated: result.payload?.caption_was_truncated === true,
    });
  } else if (
    getInternalRepostFeatureFlags().use_internal_rewrite_for_publish &&
    result.rewriteResult &&
    result.rewriteResult.parsed == null
  ) {
    logInternalRepost("info", "rewrite_fallback_to_stored_fields", {
      ...trace,
      status: "published",
      queue_item_id: result.prepared?.queue_item_id ?? id,
      provider: result.rewriteResult?.provider ?? "openrouter",
      model: result.rewriteResult?.model ?? getOpenRouterConfig().model,
      caption_length: result.payload?.caption_length ?? result.payload?.caption?.length ?? 0,
      caption_was_truncated: result.payload?.caption_was_truncated === true,
    });
  } else if (result.publishResponse?.success === true) {
    logInternalRepost("info", "publish_success", {
      ...trace,
      status: "published",
      queue_item_id: result.prepared?.queue_item_id ?? id,
      social_account_id: result.publishResponse?.social_account_id ?? null,
      caption_length: result.payload?.caption_length ?? result.payload?.caption?.length ?? 0,
      caption_was_truncated: result.payload?.caption_was_truncated === true,
    });
  } else {
    logInternalRepost("warn", "publish_failed", {
      ...trace,
      status: "publish_failed",
      queue_item_id: result.prepared?.queue_item_id ?? id,
      message: result.message,
      caption_length: result.payload?.caption_length ?? result.payload?.caption?.length ?? 0,
      caption_was_truncated: result.payload?.caption_was_truncated === true,
    });
  }

  if (!dryRun && getInternalRepostFeatureFlags().use_internal_publish_mark_status) {
    statusUpdate = await applyInternalPublishStatusUpdate({
      id,
      workerId,
      publishResponse: result.publishResponse,
      dryRun,
    });
    if (statusUpdate) {
      logInternalRepost(
        statusUpdate.httpStatus >= 400 ? "warn" : "info",
        result.publishResponse?.success === true ? "publish_mark_success" : "publish_mark_failure",
        {
          ...trace,
          status: statusUpdate.httpStatus >= 400 ? "error" : result.publishResponse?.success === true ? "done" : "failed",
          queue_item_id: result.prepared?.queue_item_id ?? id,
          message:
            statusUpdate.body?.message ||
            statusUpdate.body?.error ||
            (result.publishResponse?.success === true
              ? "Internal repost success status applied"
              : "Internal repost failure status applied"),
        }
      );
    }
  }

  return res.status(result.httpStatus).json({
    ok: result.publishResponse?.success === true,
    status: result.status,
    processing_engine: "internal",
    action: "publish",
    trace_id: trace.traceId,
    queue_item_id: result.prepared?.queue_item_id ?? id,
    publish_result: result.publishResponse,
    rewrite_result: result.rewriteResult,
    payload: result.payload,
    status_update: statusUpdate
      ? {
          ok: statusUpdate.httpStatus < 400,
          http_status: statusUpdate.httpStatus,
          result: statusUpdate.body,
        }
      : null,
    dry_run: dryRun,
    message: result.message,
  });
});

app.get("/internal/repost/dashboard-status", workerAuth, async (req, res) => {
  const runtimeConfig = getInternalRepostRuntimeConfig();
  const schedulerStatus = internalRepostScheduler.getStatus();
  const lastRunResult = schedulerStatus.last_run_result || null;
  const modeSummary = buildInternalRepostModeSummary(runtimeConfig, schedulerStatus);

  return res.json({
    backend_reachable: true,
    scheduler_enabled: runtimeConfig.featureFlags.use_internal_scheduler,
    full_repost_flow_enabled: runtimeConfig.featureFlags.use_internal_full_repost_flow,
    feature_flags: buildDashboardFeatureFlagsResponse(),
    current_mode: modeSummary.dashboardMode,
    configured_internal_repost_mode: runtimeConfig.operatingMode,
    configured_internal_repost_mode_source: runtimeConfig.operatingModeSource,
    primary_publishing_path: modeSummary.primaryPublishingPath,
    internal_repost_mode: modeSummary.internalRepostMode,
    status_badges: modeSummary.statusBadges,
    status_message: modeSummary.statusMessage,
    manual_actions: buildInternalRepostManualActions(runtimeConfig.configuredFeatureFlags),
    scheduler_interval_ms: runtimeConfig.scheduler.intervalMs,
    max_runs_per_tick: runtimeConfig.scheduler.maxRunsPerTick,
    concurrency: runtimeConfig.scheduler.concurrency,
    worker_id: runtimeConfig.scheduler.workerId,
    lease_seconds: runtimeConfig.scheduler.leaseSeconds,
    retry_seconds: runtimeConfig.scheduler.retrySeconds,
    last_run_time: schedulerStatus.last_tick_at,
    last_run_result: lastRunResult,
    last_success_time: schedulerStatus.last_success_at,
    last_failure_time: schedulerStatus.last_failure_at,
    active_scheduler_path: modeSummary.activeSchedulerPath,
    auto_processing_enabled: modeSummary.autoProcessingEnabled,
    scheduler_state: schedulerStatus,
  });
});

app.get("/internal/repost/dashboard-health", workerAuth, async (req, res) => {
  const health = await buildDashboardHealth();
  return res.json(health);
});

app.get("/internal/repost/config", workerAuth, async (req, res) => {
  return res.json(buildDashboardConfigResponse());
});

app.get("/internal/repost/dashboard-config", workerAuth, async (req, res) => {
  return res.json(buildDashboardConfigResponse());
});

async function handleRepostConfigSave(req, res) {
  const { errors, updates } = validateDashboardConfigPayload(req.body);
  if (errors.length > 0) {
    return res.status(400).json({
      ok: false,
      error: "Validation failed",
      message: "Dashboard config validation failed",
      details: errors,
    });
  }

  try {
    await saveDashboardConfig({
      updates,
      updatedBy:
        normalizeOptionalString(req.user?.email) ||
        normalizeOptionalString(req.body?.worker_id) ||
        "dashboard",
    });
    await syncInternalRepostSchedulerLifecycle();

    return res.json({
      ok: true,
      saved: true,
      message: "Repost configuration saved successfully",
      config_save_supported: true,
      config_editable: true,
      config: buildDashboardConfigResponse(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: "Server error",
      message: error.message || "Failed to save dashboard repost config",
    });
  }
}

app.post("/internal/repost/config/save", workerAuth, handleRepostConfigSave);

app.post("/internal/repost/dashboard-config", workerAuth, handleRepostConfigSave);

app.get("/internal/repost/recent-activity", workerAuth, async (req, res) => {
  const limit = Math.min(
    Math.max(normalizeOptionalNonNegativeInteger(req.query.limit) || 20, 1),
    INTERNAL_REPOST_ACTIVITY_LIMIT
  );
  return res.json({
    items: internalRepostActivity.slice(0, limit),
    total: Math.min(internalRepostActivity.length, limit),
    source: "in_memory_internal_repost_activity",
  });
});

app.post("/internal/repost/test/rewrite", workerAuth, async (req, res) => {
  const requestContext = {
    ...getManualActionRequestContext(req.body),
    action: "rewrite",
  };
  logManualTestRouteDebug("manual_test_route_hit", "rewrite", req, {
    resolver_path: "manual_test_rewrite",
    queue_item_id: requestContext.queueItemId,
    product_id: requestContext.productId,
    worker_id: requestContext.workerId,
    lease_seconds: requestContext.leaseSeconds,
  });
  const resolvedTarget = await resolveManualActionQueueItem(requestContext);
  logManualTestRouteDebug("manual_test_target_resolved", "rewrite", req, {
    resolver_path: "resolveManualActionQueueItem",
    resolved_ok: resolvedTarget.ok,
    queue_item_id: resolvedTarget.queueItemId,
    product_id: resolvedTarget.productId,
    resolved_by: resolvedTarget.resolvedBy,
    error_status: resolvedTarget.errorResponse?.body?.status || null,
  });
  if (!resolvedTarget.ok) {
    return res
      .status(resolvedTarget.errorResponse.httpStatus)
      .json(resolvedTarget.errorResponse.body);
  }

  const claimResult = await claimSpecificQueueItemForManualAction({
    id: resolvedTarget.queueItemId,
    workerId: requestContext.workerId,
    leaseSeconds: requestContext.leaseSeconds,
  });
  if (claimResult.httpStatus >= 400) {
    return res.status(claimResult.httpStatus).json({
      ok: false,
      status: "error",
      action: "rewrite",
      queue_item_id: resolvedTarget.queueItemId,
      product_id: resolvedTarget.productId,
      resolved_by: resolvedTarget.resolvedBy,
      message: claimResult.body?.message || claimResult.body?.error || "Failed to acquire queue item",
    });
  }

  const result = await rewriteClaimedRepostContext({
    id: resolvedTarget.queueItemId,
    workerId: requestContext.workerId,
  });
  return res.status(result.httpStatus).json({
    ok: result.status === "rewritten",
    status: result.status,
    processing_engine: "internal",
    action: "rewrite",
    queue_item_id: resolvedTarget.queueItemId,
    product_id: resolvedTarget.productId,
    resolved_by: resolvedTarget.resolvedBy,
    rewrite_result: result.rewriteResult,
    prepared: result.prepared,
    message: result.status === "rewritten" ? "Rewrite test completed" : result.message,
  });
});

app.post("/internal/repost/test/build-payload", workerAuth, async (req, res) => {
  const requestContext = {
    ...getManualActionRequestContext(req.body),
    action: "build_payload",
  };
  logManualTestRouteDebug("manual_test_route_hit", "build_payload", req, {
    resolver_path: "manual_test_build_payload",
    queue_item_id: requestContext.queueItemId,
    product_id: requestContext.productId,
    worker_id: requestContext.workerId,
    lease_seconds: requestContext.leaseSeconds,
  });
  const resolvedTarget = await resolveManualActionQueueItem(requestContext);
  logManualTestRouteDebug("manual_test_target_resolved", "build_payload", req, {
    resolver_path: "resolveManualActionQueueItem",
    resolved_ok: resolvedTarget.ok,
    queue_item_id: resolvedTarget.queueItemId,
    product_id: resolvedTarget.productId,
    resolved_by: resolvedTarget.resolvedBy,
    error_status: resolvedTarget.errorResponse?.body?.status || null,
  });
  if (!resolvedTarget.ok) {
    return res
      .status(resolvedTarget.errorResponse.httpStatus)
      .json(resolvedTarget.errorResponse.body);
  }

  const claimResult = await claimSpecificQueueItemForManualAction({
    id: resolvedTarget.queueItemId,
    workerId: requestContext.workerId,
    leaseSeconds: requestContext.leaseSeconds,
  });
  if (claimResult.httpStatus >= 400) {
    return res.status(claimResult.httpStatus).json({
      ok: false,
      status: "error",
      action: "build_payload",
      queue_item_id: resolvedTarget.queueItemId,
      product_id: resolvedTarget.productId,
      resolved_by: resolvedTarget.resolvedBy,
      message: claimResult.body?.message || claimResult.body?.error || "Failed to acquire queue item",
    });
  }

  const result = await buildInternalPublishPayloadFromQueueItem({
    id: resolvedTarget.queueItemId,
    workerId: requestContext.workerId,
  });
  return res.status(result.httpStatus).json({
    ok: result.httpStatus < 400,
    status: result.status,
    processing_engine: "internal",
    action: "build_payload",
    queue_item_id: resolvedTarget.queueItemId,
    product_id: resolvedTarget.productId,
    resolved_by: resolvedTarget.resolvedBy,
    payload: result.payload,
    prepared: result.prepared,
    message:
      result.httpStatus < 400 ? "Build payload test completed" : result.message,
  });
});

app.post("/internal/repost/test/publish", workerAuth, async (req, res) => {
  const requestContext = {
    ...getManualActionRequestContext(req.body),
    action: "publish",
  };
  logManualTestRouteDebug("manual_test_route_hit", "publish", req, {
    resolver_path: "manual_test_publish",
    queue_item_id: requestContext.queueItemId,
    product_id: requestContext.productId,
    worker_id: requestContext.workerId,
    lease_seconds: requestContext.leaseSeconds,
    dry_run_requested: requestContext.dryRunRequested,
    has_rewritten_fields: requestContext.rewrittenFields != null,
  });
  const resolvedTarget = await resolveManualActionQueueItem(requestContext);
  logManualTestRouteDebug("manual_test_target_resolved", "publish", req, {
    resolver_path: "resolveManualActionQueueItem",
    resolved_ok: resolvedTarget.ok,
    queue_item_id: resolvedTarget.queueItemId,
    product_id: resolvedTarget.productId,
    resolved_by: resolvedTarget.resolvedBy,
    error_status: resolvedTarget.errorResponse?.body?.status || null,
  });
  if (!resolvedTarget.ok) {
    return res
      .status(resolvedTarget.errorResponse.httpStatus)
      .json(resolvedTarget.errorResponse.body);
  }

  const claimResult = await claimSpecificQueueItemForManualAction({
    id: resolvedTarget.queueItemId,
    workerId: requestContext.workerId,
    leaseSeconds: requestContext.leaseSeconds,
  });
  if (claimResult.httpStatus >= 400) {
    return res.status(claimResult.httpStatus).json({
      ok: false,
      status: "error",
      action: "publish",
      queue_item_id: resolvedTarget.queueItemId,
      product_id: resolvedTarget.productId,
      resolved_by: resolvedTarget.resolvedBy,
      message: claimResult.body?.message || claimResult.body?.error || "Failed to acquire queue item",
    });
  }

  const runtimeConfig = getInternalRepostRuntimeConfig();
  const effectiveDryRun = runtimeConfig.scheduler.dryRun || requestContext.dryRunRequested;
  const result = await executeInternalPublishForQueueItem({
    id: resolvedTarget.queueItemId,
    workerId: requestContext.workerId,
    dryRun: effectiveDryRun,
    rewrittenFields: requestContext.rewrittenFields,
  });

  let statusUpdate = null;
  if (!effectiveDryRun && getInternalRepostFeatureFlags().use_internal_publish_mark_status) {
    statusUpdate = await applyInternalPublishStatusUpdate({
      id: resolvedTarget.queueItemId,
      workerId: requestContext.workerId,
      publishResponse: result.publishResponse,
      dryRun: effectiveDryRun,
    });
  }

  return res.status(result.httpStatus).json({
    ok: result.publishResponse?.success === true,
    status: result.status,
    processing_engine: "internal",
    action: "publish",
    queue_item_id: resolvedTarget.queueItemId,
    product_id: resolvedTarget.productId,
    resolved_by: resolvedTarget.resolvedBy,
    dry_run: effectiveDryRun,
    publish_result: result.publishResponse,
    rewrite_result: result.rewriteResult,
    payload: result.payload,
    status_update: statusUpdate
      ? {
          ok: statusUpdate.httpStatus < 400,
          http_status: statusUpdate.httpStatus,
          result: statusUpdate.body,
        }
      : null,
    message:
      result.publishResponse?.success === true
        ? "Publish test completed"
        : result.message,
  });
});

app.get("/internal/repost/scheduler/status", workerAuth, async (req, res) => {
  return res.json(internalRepostScheduler.getStatus());
});

app.post("/internal/repost/scheduler/run-now", workerAuth, async (req, res) => {
  logInternalRepost("info", "run_now_requested", {
    action: "scheduler",
    processing_engine: "internal",
    requested_by: req.body?.worker_id || "internal-repost",
  });

  const result = await internalRepostScheduler.runNow();
  return res.json(result);
});



app.post("/worker/recover-stuck", workerAuth, async (req, res) => {

  const r = await pool.query(

    `

    UPDATE products_queue

    SET status='scheduled',

        locked_by=NULL,

        locked_at=NULL,

        lease_expires_at=NULL,
        processing_started_at=NULL,

        next_run_at=NOW(),

        updated_at=NOW()

    WHERE status='processing'

      AND lease_expires_at IS NOT NULL

      AND lease_expires_at <= NOW()

    RETURNING id

    `

  );

  return res.json({ recovered: r.rowCount, ids: r.rows.map(x => x.id) });

});



app.post("/worker/heartbeat/:id", workerAuth, async (req,res)=>{const id=Number(req.params.id);const workerId=req.body?.worker_id||"worker-1";const leaseSeconds=Math.min(Math.max(Number(req.body?.lease_seconds||60),10),600);if(!Number.isFinite(id))return res.status(400).json({error:"Invalid id"});const r=await pool.query("UPDATE products_queue SET lease_expires_at=NOW()+($3||' seconds')::interval, updated_at=NOW() WHERE id=$1 AND status='processing' AND locked_by=$2 RETURNING id,status,locked_by,lease_expires_at",[id,workerId,String(leaseSeconds)]);if(r.rowCount===0)return res.status(409).json({error:"Not owned by this worker"});return res.json({item:r.rows[0]});});



app.get("/media/:filename", async (req, res) => {

  const requestedFilename = normalizeRequestedMediaFilename(req.params.filename);

  if (!requestedFilename) {

    return res.status(400).json({ error: "Invalid media filename" });

  }

  try {

    let mediaFilePath = await resolveCachedMediaFilePath(requestedFilename);

    if (!mediaFilePath) {

      mediaFilePath = await cacheMediaFileFromWebhookSource(requestedFilename);

    }

    if (!mediaFilePath) {

      return res.status(404).json({ error: "Media file not found" });

    }

    res.set("Cache-Control", "public, max-age=86400");
    return res.sendFile(mediaFilePath);

  } catch (error) {

    console.error("[media] failed to serve media file", {
      filename: requestedFilename,
      message: error?.message || String(error),
    });
    return res.status(502).json({ error: "Failed to resolve media file" });

  }

});

// ---- 404 fallback ----



app.use((req, res) => {

  res.status(404).json({ error: "Not Found", path: req.path });

});



const PORT = Number(process.env.PORT || 3000);

function logTrackedInternalRepostRoutes() {
  const trackedPaths = new Set([
    "/internal/repost/test/rewrite",
    "/internal/repost/test/build-payload",
    "/internal/repost/test/publish",
    "/internal/repost/:id/rewrite",
    "/internal/repost/:id/build-payload",
    "/internal/repost/:id/publish",
  ]);
  const routerStack = Array.isArray(app.router?.stack) ? app.router.stack : [];
  const routeSnapshot = routerStack
    .map((layer, index) => {
      const routePath = layer?.route?.path;
      if (!routePath || !trackedPaths.has(routePath)) {
        return null;
      }

      return {
        index,
        path: routePath,
        methods: Object.keys(layer.route.methods || {}).filter((method) => layer.route.methods[method]),
      };
    })
    .filter(Boolean);

  console.log("[internal-repost-debug] tracked_route_snapshot", routeSnapshot);
}

async function start() {

  try {

    await ensureSocialAccountsTable();
    await ensureInboundWebhookTables();
    await ensureProductsQueueImageRotationState();
    await ensureRuntimeSettingsTable();
    await loadRuntimeSettingsFromDb();
    logInstagramPublishConfigStatus();
    app.listen(PORT, "127.0.0.1", () => {

      console.log(`publisher-api listening on http://127.0.0.1:${PORT}`);
      logTrackedInternalRepostRoutes();
      try {
        internalRepostScheduler.start();
      } catch (schedulerError) {
        logInternalRepost("error", "scheduler_init_failed", {
          action: "scheduler",
          processing_engine: "internal",
          message: schedulerError?.message || String(schedulerError),
        });
      }

    });

  } catch (e) {

    console.error(`Failed to start publisher-api: ${e.message}`);
    process.exit(1);

  }

}

await start();
