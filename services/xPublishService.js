import axios from "axios";
import { attachNormalizedPublishResult } from "./publishResult.js";

const DEFAULT_X_API_BASE_URL = "https://api.x.com";
const DEFAULT_X_UPLOAD_BASE_URL = "https://upload.twitter.com/1.1";
const DEFAULT_X_TIMEOUT_MS = 15000;
const MAX_X_MEDIA_ATTACHMENTS = 4;

function normalizeOptionalString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function createXPublishError(
  message,
  code,
  type,
  socialAccountId = null,
  details = undefined
) {
  return attachNormalizedPublishResult({
    success: false,
    platform: "x",
    social_account_id: socialAccountId,
    publish_result: null,
    error: {
      message,
      code: code == null ? null : String(code),
      type: type || "publish_error",
      ...(details ? { details } : {}),
    },
  });
}

function buildBearerHeaders(accessToken, headers = {}) {
  return {
    Authorization: `Bearer ${accessToken}`,
    ...headers,
  };
}

function extractXErrorMessage(data, fallbackMessage) {
  if (Array.isArray(data?.errors) && data.errors.length > 0) {
    const firstError = data.errors[0];
    return (
      normalizeOptionalString(firstError?.detail) ||
      normalizeOptionalString(firstError?.message) ||
      normalizeOptionalString(firstError?.title) ||
      fallbackMessage
    );
  }

  return (
    normalizeOptionalString(data?.detail) ||
    normalizeOptionalString(data?.message) ||
    fallbackMessage
  );
}

function normalizeMediaUrls(payload) {
  const urls = Array.isArray(payload?.normalized_x_media_urls)
    ? payload.normalized_x_media_urls
    : [];

  return urls
    .map((value) => normalizeOptionalString(value))
    .filter(Boolean)
    .slice(0, MAX_X_MEDIA_ATTACHMENTS);
}

function chooseBaseText(payload) {
  return (
    normalizeOptionalString(payload?.x_text) ||
    normalizeOptionalString(payload?.tweet_text) ||
    normalizeOptionalString(payload?.post_text)
  );
}

export function buildXPostText(payload) {
  const text = chooseBaseText(payload);
  const link = normalizeOptionalString(payload?.x_link);

  if (!link) {
    return text;
  }

  if (!text) {
    return link;
  }

  if (text.includes(link)) {
    return text;
  }

  return `${text}\n\n${link}`;
}

export function buildXCreateTweetBody(payload, mediaIds = []) {
  const text = buildXPostText(payload);
  const body = {};

  if (text) {
    body.text = text;
  }

  if (Array.isArray(mediaIds) && mediaIds.length > 0) {
    body.media = {
      media_ids: mediaIds.map((value) => String(value)),
    };
  }

  return body;
}

async function downloadRemoteMedia(mediaUrl, httpClient) {
  const response = await httpClient.get(mediaUrl, {
    responseType: "arraybuffer",
    timeout: DEFAULT_X_TIMEOUT_MS,
    maxRedirects: 5,
  });

  const contentType = normalizeOptionalString(response.headers?.["content-type"]);
  if (!/^image\//i.test(contentType)) {
    throw new Error(
      `Remote URL returned non-image content-type: ${contentType || "unknown"}`
    );
  }

  return {
    buffer: Buffer.from(response.data),
    contentType,
  };
}

async function uploadXMedia(accessToken, media, config, httpClient) {
  const form = new URLSearchParams();
  form.set("media_category", "tweet_image");
  form.set("media_data", media.buffer.toString("base64"));

  const response = await httpClient.post(
    `${config.uploadBaseUrl}/media/upload.json`,
    form.toString(),
    {
      headers: buildBearerHeaders(accessToken, {
        "Content-Type": "application/x-www-form-urlencoded",
      }),
      timeout: config.timeoutMs,
    }
  );

  const mediaId = String(
    response?.data?.media_id_string || response?.data?.media_id || ""
  ).trim();

  if (!mediaId) {
    throw new Error("X media upload did not return a media id");
  }

  return mediaId;
}

function buildXPostUrl(account, tweetId) {
  const handle = normalizeOptionalString(account?.handle).replace(/^@+/, "");
  if (handle) {
    return `https://x.com/${handle}/status/${tweetId}`;
  }

  return `https://x.com/i/web/status/${tweetId}`;
}

function buildXPublishConfig(runtimeConfig = {}) {
  return {
    apiBaseUrl:
      normalizeOptionalString(runtimeConfig.apiBaseUrl) || DEFAULT_X_API_BASE_URL,
    uploadBaseUrl:
      normalizeOptionalString(runtimeConfig.uploadBaseUrl) ||
      DEFAULT_X_UPLOAD_BASE_URL,
    timeoutMs:
      Number.isFinite(runtimeConfig.timeoutMs) && runtimeConfig.timeoutMs > 0
        ? runtimeConfig.timeoutMs
        : DEFAULT_X_TIMEOUT_MS,
  };
}

export async function publishToX(payload, account, options = {}) {
  const socialAccountId =
    Number.isInteger(account?.id) && account.id > 0 ? account.id : null;
  const accessToken = normalizeOptionalString(account?.access_token);
  const text = buildXPostText(payload);
  const mediaUrls = normalizeMediaUrls(payload);
  const config = buildXPublishConfig(options.config);
  const httpClient = options.httpClient || axios;
  const now = typeof options.now === "function" ? options.now : () => new Date();

  if (!accessToken) {
    return createXPublishError(
      "X social account is missing access_token",
      "ACCESS_TOKEN_MISSING",
      "account_error",
      socialAccountId
    );
  }

  if (!text && mediaUrls.length === 0) {
    return createXPublishError(
      "X publish requires text or media_urls",
      "TEXT_OR_MEDIA_REQUIRED",
      "validation_error",
      socialAccountId
    );
  }

  try {
    const mediaIds = [];

    for (const mediaUrl of mediaUrls) {
      const media = await downloadRemoteMedia(mediaUrl, httpClient);
      const mediaId = await uploadXMedia(accessToken, media, config, httpClient);
      mediaIds.push(mediaId);
    }

    const requestBody = buildXCreateTweetBody(payload, mediaIds);
    const response = await httpClient.post(
      `${config.apiBaseUrl}/2/tweets`,
      requestBody,
      {
        headers: buildBearerHeaders(accessToken, {
          "Content-Type": "application/json",
        }),
        timeout: config.timeoutMs,
      }
    );

    const tweetId = String(response?.data?.data?.id || "").trim();
    if (!tweetId) {
      return createXPublishError(
        extractXErrorMessage(
          response?.data,
          "X API did not return a tweet id"
        ),
        "MISSING_TWEET_ID",
        "x_api_error",
        socialAccountId
      );
    }

    return attachNormalizedPublishResult({
      success: true,
      platform: "x",
      social_account_id: socialAccountId,
      publish_result: {
        external_id: tweetId,
        published_at: now().toISOString(),
        url: buildXPostUrl(account, tweetId),
        text: requestBody.text || "",
      },
      error: null,
    });
  } catch (error) {
    const providerStatus = error?.response?.status ?? null;
    const providerData = error?.response?.data;

    return createXPublishError(
      extractXErrorMessage(providerData, error?.message || "X publish failed"),
      providerStatus || error?.code || "X_PUBLISH_FAILED",
      providerStatus ? "x_api_error" : "network_error",
      socialAccountId
    );
  }
}
