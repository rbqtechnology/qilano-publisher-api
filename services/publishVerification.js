function normalizePlatform(value) {
  return typeof value === "string" && value.trim()
    ? value.trim().toLowerCase()
    : "";
}

function normalizePublishedAt(value) {
  const publishedAt = typeof value === "string" ? value.trim() : "";
  return publishedAt ? publishedAt : "";
}

function normalizeUrl(value) {
  const url = typeof value === "string" ? value.trim() : "";
  return url ? url : "";
}

function isInstagramUrl(url) {
  return /https?:\/\/(www\.)?instagram\.com\//i.test(url);
}

function isXUrl(url) {
  return /https?:\/\/(www\.)?(x|twitter)\.com\//i.test(url);
}

export function normalizeConfirmedPublishResponse(publishResponse) {
  if (!publishResponse || publishResponse.success !== true) {
    return null;
  }

  const platform = normalizePlatform(
    publishResponse?.platform || publishResponse?.publish_result?.platform
  );
  const externalId = String(
    publishResponse?.publish_result?.external_id || ""
  ).trim();
  const url = normalizeUrl(publishResponse?.publish_result?.url);
  const publishedAt = normalizePublishedAt(
    publishResponse?.publish_result?.published_at
  );

  if (!platform || !externalId || !url || !publishedAt) {
    return null;
  }

  if (platform === "instagram" && !isInstagramUrl(url)) {
    return null;
  }

  if (platform === "x" && !isXUrl(url)) {
    return null;
  }

  if (platform !== "instagram" && platform !== "x") {
    return null;
  }

  return {
    platform,
    externalId,
    url,
    publishedAt,
  };
}
