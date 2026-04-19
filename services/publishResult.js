function normalizeOptionalString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function normalizePlatform(value) {
  return normalizeOptionalString(value).toLowerCase();
}

export function buildNormalizedPublishResult(publishResponse) {
  const platform = normalizePlatform(
    publishResponse?.platform || publishResponse?.publish_result?.platform
  );
  const success = publishResponse?.success === true;
  const externalId = normalizeOptionalString(
    publishResponse?.publish_result?.external_id
  );
  const url = normalizeOptionalString(publishResponse?.publish_result?.url);
  const publishedAt = normalizeOptionalString(
    publishResponse?.publish_result?.published_at
  );
  const message =
    normalizeOptionalString(publishResponse?.message) ||
    normalizeOptionalString(publishResponse?.error?.message) ||
    (success && platform ? `${platform} publish succeeded` : "");

  return {
    success,
    platform: platform || null,
    external_id: externalId || null,
    url: url || null,
    published_at: publishedAt || null,
    message: message || null,
  };
}

export function attachNormalizedPublishResult(publishResponse) {
  if (!publishResponse || typeof publishResponse !== "object") {
    return publishResponse;
  }

  const normalizedPublishResult = buildNormalizedPublishResult(publishResponse);

  return {
    ...publishResponse,
    message:
      normalizeOptionalString(publishResponse.message) ||
      normalizedPublishResult.message,
    normalized_publish_result: normalizedPublishResult,
  };
}
