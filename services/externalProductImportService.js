import axios from "axios";
import crypto from "crypto";
import fetch from "node-fetch";

const DEFAULT_FETCH_TIMEOUT_MS = 12000;
const DEFAULT_FETCH_RETRIES = 2;
const DEFAULT_BROWSER_TIMEOUT_MS = 25000;
const DEFAULT_BROWSER_WAIT_AFTER_GOTO_MS = 1500;
const MAX_IMAGE_CANDIDATES = 12;
const MAX_BODY_LENGTH = 2 * 1024 * 1024;
const DESKTOP_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36";
const MOBILE_USER_AGENT =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1";

const PROVIDER_RULES = [
  { key: "etsy", domains: ["etsy.com"] },
  { key: "amazon", domains: ["amazon.com", "amzn.to"] },
  { key: "aliexpress", domains: ["aliexpress.com", "s.click.aliexpress.com"] },
];
const SUPPORTED_PROVIDER_KEYS = new Set(PROVIDER_RULES.map((rule) => rule.key));

const PROVIDER_HEADER_OVERRIDES = {
  etsy: {
    Referer: "https://www.etsy.com/",
  },
  amazon: {
    Referer: "https://www.amazon.com/",
  },
  aliexpress: {
    Referer: "https://www.aliexpress.com/",
  },
};

const PROVIDER_BROWSER_SELECTORS = {
  etsy: {
    title: [
      "h1[data-buy-box-listing-title]",
      "h1[data-listing-page-title]",
      "#listing-page-cart h1",
      "h1",
    ],
    description: [
      '[data-product-details-description-text-content]',
      '[data-id="description-text"]',
      "#description-text",
      'meta[name="description"]',
    ],
    price: [
      '[data-buy-box-region="price"] .wt-text-title-larger',
      '[data-selector="price-only"]',
      '[data-buy-box-region="price"]',
    ],
    image: [
      'img[data-index]',
      'img[data-src*="etsy"]',
      'img[src*="etsy"]',
    ],
    waitFor: [
      "h1[data-buy-box-listing-title]",
      "h1",
      'meta[property="og:title"]',
    ],
  },
  amazon: {
    title: [
      "#productTitle",
      'meta[property="og:title"]',
      "h1",
    ],
    description: [
      "#feature-bullets",
      "#productDescription",
      'meta[name="description"]',
    ],
    price: [
      "#corePrice_feature_div .a-offscreen",
      "#priceblock_ourprice",
      "#priceblock_dealprice",
      ".apexPriceToPay .a-offscreen",
    ],
    image: [
      "#landingImage",
      "#imgTagWrapperId img",
      "#altImages img",
      'img[data-old-hires]',
    ],
    waitFor: [
      "#productTitle",
      "#landingImage",
      'meta[property="og:title"]',
    ],
  },
  aliexpress: {
    title: [
      'h1[data-pl="product-title"]',
      ".product-title-text",
      "h1",
    ],
    description: [
      ".product-description",
      ".overview--content--KyqL_5R",
      'meta[name="description"]',
    ],
    price: [
      '[data-pl="product-price"]',
      ".product-price-current",
      ".snow-price_SnowPrice__mainS",
    ],
    image: [
      '.images-view-list img',
      '.magnifier--image--L4hZ4dC img',
      'img[src*="alicdn.com"]',
    ],
    waitFor: [
      'h1[data-pl="product-title"]',
      'img[src*="alicdn.com"]',
      'meta[property="og:title"]',
    ],
  },
};

let playwrightModulePromise = null;

function normalizeOptionalString(value) {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized ? normalized : null;
}

function normalizeWhitespace(value) {
  const normalized = normalizeOptionalString(value);
  return normalized ? normalized.replace(/\s+/g, " ").trim() : null;
}

function decodeHtmlEntities(value) {
  const normalized = normalizeOptionalString(value);
  if (!normalized) {
    return null;
  }

  const namedEntities = {
    amp: "&",
    apos: "'",
    quot: "\"",
    lt: "<",
    gt: ">",
    nbsp: " ",
  };

  return normalized.replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (match, entity) => {
    const lowered = String(entity).toLowerCase();

    if (namedEntities[lowered]) {
      return namedEntities[lowered];
    }

    if (lowered.startsWith("#x")) {
      const parsed = Number.parseInt(lowered.slice(2), 16);
      return Number.isFinite(parsed) ? String.fromCodePoint(parsed) : match;
    }

    if (lowered.startsWith("#")) {
      const parsed = Number.parseInt(lowered.slice(1), 10);
      return Number.isFinite(parsed) ? String.fromCodePoint(parsed) : match;
    }

    return match;
  });
}

function stripHtml(value) {
  const normalized = normalizeOptionalString(value);
  if (!normalized) {
    return null;
  }

  return normalizeWhitespace(decodeHtmlEntities(normalized.replace(/<[^>]*>/g, " ")));
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function absolutizeUrl(candidate, baseUrl) {
  const normalized = normalizeOptionalString(candidate);
  if (!normalized) {
    return null;
  }

  if (/^data:/i.test(normalized) || /^javascript:/i.test(normalized)) {
    return null;
  }

  try {
    return new URL(normalized, baseUrl).toString();
  } catch {
    return null;
  }
}

function dedupeStrings(values) {
  const output = [];
  const seen = new Set();

  for (const value of values) {
    const normalized = normalizeOptionalString(value);
    if (!normalized) {
      continue;
    }

    if (seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    output.push(normalized);
  }

  return output;
}

function findAttributeValue(tag, attributeName) {
  const pattern = new RegExp(
    `\\b${escapeRegExp(attributeName)}\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s>]+))`,
    "i"
  );
  const match = String(tag).match(pattern);
  return normalizeOptionalString(match?.[1] || match?.[2] || match?.[3] || null);
}

function extractMetaTags(html) {
  const contentByProperty = new Map();
  const contentByName = new Map();
  const tags = String(html).match(/<meta\b[^>]*>/gi) || [];

  for (const tag of tags) {
    const content = decodeHtmlEntities(findAttributeValue(tag, "content"));
    if (!content) {
      continue;
    }

    const propertyKey = normalizeOptionalString(findAttributeValue(tag, "property"));
    if (propertyKey) {
      contentByProperty.set(propertyKey.toLowerCase(), content);
    }

    const nameKey = normalizeOptionalString(findAttributeValue(tag, "name"));
    if (nameKey) {
      contentByName.set(nameKey.toLowerCase(), content);
    }
  }

  return { contentByProperty, contentByName };
}

function extractLinkHref(html, relName, baseUrl) {
  const tags = String(html).match(/<link\b[^>]*>/gi) || [];

  for (const tag of tags) {
    const rel = normalizeOptionalString(findAttributeValue(tag, "rel"));
    if (!rel) {
      continue;
    }

    const relTokens = rel.toLowerCase().split(/\s+/);
    if (!relTokens.includes(relName.toLowerCase())) {
      continue;
    }

    const href = absolutizeUrl(findAttributeValue(tag, "href"), baseUrl);
    if (href) {
      return href;
    }
  }

  return null;
}

function extractTitleTag(html) {
  const match = String(html).match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return normalizeWhitespace(decodeHtmlEntities(match?.[1] || null));
}

function extractJsonLdBlocks(html) {
  const blocks = [];
  const pattern = /<script\b[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match = pattern.exec(String(html));

  while (match) {
    const raw = normalizeOptionalString(match[1]);
    if (raw) {
      try {
        blocks.push(JSON.parse(raw));
      } catch {
        // Ignore malformed JSON-LD blocks.
      }
    }

    match = pattern.exec(String(html));
  }

  return blocks;
}

function flattenJsonLdNodes(value) {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return value.flatMap((entry) => flattenJsonLdNodes(entry));
  }

  if (typeof value !== "object") {
    return [];
  }

  const graphNodes = Array.isArray(value["@graph"]) ? value["@graph"] : [];
  return [value, ...graphNodes.flatMap((entry) => flattenJsonLdNodes(entry))];
}

function normalizeSchemaType(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry).toLowerCase());
  }

  if (value == null) {
    return [];
  }

  return [String(value).toLowerCase()];
}

function pickProductNode(jsonLdBlocks) {
  const nodes = jsonLdBlocks.flatMap((block) => flattenJsonLdNodes(block));

  for (const node of nodes) {
    const schemaTypes = normalizeSchemaType(node?.["@type"]);
    if (schemaTypes.some((entry) => entry.endsWith("product"))) {
      return node;
    }
  }

  return null;
}

function normalizeImageCollection(value, baseUrl) {
  if (value == null) {
    return [];
  }

  if (Array.isArray(value)) {
    return dedupeStrings(
      value.flatMap((entry) => normalizeImageCollection(entry, baseUrl))
    );
  }

  if (typeof value === "object") {
    return normalizeImageCollection(
      value.url || value.contentUrl || value.image || value.src || null,
      baseUrl
    );
  }

  const absolute = absolutizeUrl(value, baseUrl);
  return absolute ? [absolute] : [];
}

function extractImgTagCandidates(html, baseUrl) {
  const tags = String(html).match(/<img\b[^>]*>/gi) || [];
  const candidates = [];

  for (const tag of tags) {
    const src =
      absolutizeUrl(findAttributeValue(tag, "src"), baseUrl) ||
      absolutizeUrl(findAttributeValue(tag, "data-src"), baseUrl) ||
      absolutizeUrl(findAttributeValue(tag, "data-original"), baseUrl) ||
      absolutizeUrl(findAttributeValue(tag, "data-zoom-image"), baseUrl) ||
      absolutizeUrl(findAttributeValue(tag, "data-old-hires"), baseUrl);

    if (!src) {
      continue;
    }

    candidates.push(src);
    if (candidates.length >= MAX_IMAGE_CANDIDATES) {
      break;
    }
  }

  return dedupeStrings(candidates);
}

function extractPriceFromProductNode(productNode) {
  if (!productNode || typeof productNode !== "object") {
    return { amount: null, currency: null };
  }

  const offers = Array.isArray(productNode.offers)
    ? productNode.offers[0]
    : productNode.offers;

  if (!offers || typeof offers !== "object") {
    return { amount: null, currency: null };
  }

  const amount = normalizeOptionalString(
    offers.priceSpecification?.price || offers.price || null
  );
  const currency = normalizeOptionalString(
    offers.priceSpecification?.priceCurrency || offers.priceCurrency || null
  );

  return { amount, currency };
}

function buildCaption(title, description, price) {
  const normalizedTitle = normalizeWhitespace(title);
  const normalizedDescription = normalizeWhitespace(description);
  const normalizedPrice = normalizeWhitespace(price);

  if (normalizedTitle && normalizedDescription && normalizedDescription !== normalizedTitle) {
    return normalizedPrice
      ? `${normalizedTitle}\n\n${normalizedDescription}\n\nPrice: ${normalizedPrice}`
      : `${normalizedTitle}\n\n${normalizedDescription}`;
  }

  if (normalizedTitle && normalizedPrice) {
    return `${normalizedTitle}\n\nPrice: ${normalizedPrice}`;
  }

  return normalizedTitle || normalizedDescription || normalizedPrice || null;
}

function buildGeneratedExternalProductId(canonicalUrl) {
  const digest = crypto.createHash("sha1").update(String(canonicalUrl)).digest("hex").slice(0, 16);
  return `external-url-${digest}`;
}

function detectProvider(hostname, providerHint) {
  const normalizedHostname = normalizeOptionalString(hostname)?.toLowerCase() || "";
  const normalizedHint = normalizeOptionalString(providerHint)?.toLowerCase() || null;

  if (normalizedHint) {
    return normalizedHint;
  }

  for (const rule of PROVIDER_RULES) {
    if (
      rule.domains.some(
        (domain) => normalizedHostname === domain || normalizedHostname.endsWith(`.${domain}`)
      )
    ) {
      return rule.key;
    }
  }

  return normalizedHostname || "external";
}

function buildPreview({
  title,
  description,
  images,
  canonicalUrl,
  provider,
  sourceDomain,
  externalProductId,
  priceAmount,
  priceCurrency,
  metadata,
}) {
  const thumbnail = Array.isArray(images) && images.length > 0 ? images[0] : null;

  return {
    title: title || null,
    description: description || null,
    images: images || [],
    image_count: Array.isArray(images) ? images.length : 0,
    thumbnail,
    canonical_source_url: canonicalUrl,
    source_domain: sourceDomain,
    provider,
    external_id: externalProductId,
    price: priceAmount
      ? {
          amount: priceAmount,
          currency: priceCurrency || null,
        }
      : null,
    metadata,
  };
}

function validateExtractedProduct({ title, images, canonicalUrl }) {
  const errors = [];

  if (!canonicalUrl) {
    errors.push("Unable to determine a canonical source URL");
  }

  if (!normalizeOptionalString(title)) {
    errors.push("Unable to extract a product title");
  }

  if (!Array.isArray(images) || images.length === 0) {
    errors.push("Unable to extract at least one product image");
  }

  return errors;
}

function extractJsonAssignment(html, patterns) {
  for (const pattern of patterns) {
    const match = String(html).match(pattern);
    const raw = normalizeOptionalString(match?.[1]);
    if (!raw) {
      continue;
    }

    try {
      return JSON.parse(raw);
    } catch {
      continue;
    }
  }

  return null;
}

function detectBlockedReason(html, status) {
  const normalizedHtml = String(html || "").toLowerCase();

  if (status === 403) {
    if (
      normalizedHtml.includes("captcha") ||
      normalizedHtml.includes("robot check") ||
      normalizedHtml.includes("verify you are a human")
    ) {
      return "captcha";
    }

    if (
      normalizedHtml.includes("access denied") ||
      normalizedHtml.includes("forbidden") ||
      normalizedHtml.includes("blocked") ||
      normalizedHtml.includes("challenge")
    ) {
      return "blocked";
    }

    return "forbidden";
  }

  if (status === 429) {
    return "rate_limited";
  }

  if (
    normalizedHtml.includes("captcha") ||
    normalizedHtml.includes("robot check") ||
    normalizedHtml.includes("verify you are a human")
  ) {
    return "captcha";
  }

  if (
    normalizedHtml.includes("access denied") ||
    normalizedHtml.includes("blocked") ||
    normalizedHtml.includes("challenge")
  ) {
    return "blocked";
  }

  return null;
}

function buildRequestHeaders(targetUrl, provider, userAgent) {
  const parsedUrl = new URL(targetUrl);
  const providerHeaders = PROVIDER_HEADER_OVERRIDES[provider] || {};

  return {
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Referer: providerHeaders.Referer || `${parsedUrl.protocol}//${parsedUrl.host}/`,
    "Sec-Ch-Ua":
      "\"Chromium\";v=\"136\", \"Google Chrome\";v=\"136\", \"Not.A/Brand\";v=\"99\"",
    "Sec-Ch-Ua-Mobile": userAgent === MOBILE_USER_AGENT ? "?1" : "?0",
    "Sec-Ch-Ua-Platform": userAgent === MOBILE_USER_AGENT ? "\"iOS\"" : "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": userAgent,
  };
}

function buildFetchAttempts(targetUrl, provider) {
  return [
    {
      client: "axios",
      label: "axios_desktop",
      headers: buildRequestHeaders(targetUrl, provider, DESKTOP_USER_AGENT),
    },
    {
      client: "axios",
      label: "axios_mobile",
      headers: buildRequestHeaders(targetUrl, provider, MOBILE_USER_AGENT),
    },
    {
      client: "fetch",
      label: "fetch_desktop",
      headers: buildRequestHeaders(targetUrl, provider, DESKTOP_USER_AGENT),
    },
  ];
}

function buildFailureReport(attempts) {
  if (!Array.isArray(attempts) || attempts.length === 0) {
    return {
      reason: "fetch_failed",
      message: "Unable to fetch external product URL",
      attempts: [],
    };
  }

  const statusAttempts = attempts.filter((attempt) => Number.isInteger(attempt.status));
  const blockedAttempt =
    statusAttempts.find((attempt) => attempt.blocked_reason === "captcha") ||
    statusAttempts.find((attempt) => attempt.blocked_reason === "blocked") ||
    statusAttempts.find((attempt) => attempt.blocked_reason === "forbidden") ||
    statusAttempts.find((attempt) => attempt.blocked_reason === "rate_limited");

  if (blockedAttempt) {
    const reason = blockedAttempt.blocked_reason;
    const reasonMessage =
      reason === "captcha"
        ? "External site blocked the request with a captcha challenge"
        : reason === "rate_limited"
          ? "External site rate-limited the request"
          : "External site blocked the request";

    return {
      reason,
      message: `${reasonMessage} (${blockedAttempt.status}${blockedAttempt.status_text ? ` ${blockedAttempt.status_text}` : ""})`,
      attempts,
    };
  }

  const serverAttempt = statusAttempts.find((attempt) => attempt.status >= 500);
  if (serverAttempt) {
    return {
      reason: "upstream_error",
      message: `External site returned an upstream error (${serverAttempt.status}${serverAttempt.status_text ? ` ${serverAttempt.status_text}` : ""})`,
      attempts,
    };
  }

  const timeoutAttempt = attempts.find((attempt) => attempt.reason === "timeout");
  if (timeoutAttempt) {
    return {
      reason: "timeout",
      message: "External site did not respond before the fetch timeout",
      attempts,
    };
  }

  const browserAttempt = attempts.find((attempt) => attempt.reason === "browser_unavailable");
  if (browserAttempt) {
    return {
      reason: "browser_unavailable",
      message: "Playwright browser fallback is unavailable on this server",
      attempts,
    };
  }

  const browserFailureAttempt = attempts.find((attempt) => attempt.reason === "browser_failed");
  if (browserFailureAttempt) {
    return {
      reason: "browser_failed",
      message: "Playwright browser fallback failed to extract the page",
      attempts,
    };
  }

  const networkAttempt = attempts.find((attempt) => attempt.reason === "network_error");
  if (networkAttempt) {
    return {
      reason: "network_error",
      message: "Unable to reach external product URL",
      attempts,
    };
  }

  const weakAttempt = attempts.find((attempt) => attempt.reason === "weak_extraction");
  if (weakAttempt) {
    return {
      reason: "weak_extraction",
      message: "External product page loaded but did not contain enough metadata",
      attempts,
    };
  }

  const lastAttempt = attempts[attempts.length - 1];
  return {
    reason: lastAttempt?.reason || "fetch_failed",
    message: "Unable to fetch external product URL",
    attempts,
  };
}

function extractEtsyCandidateData(html, baseUrl) {
  const meta = extractMetaTags(html);
  const ld = pickProductNode(extractJsonLdBlocks(html));

  return {
    title: normalizeWhitespace(
      decodeHtmlEntities(
        ld?.name ||
          meta.contentByProperty.get("og:title") ||
          meta.contentByName.get("twitter:title") ||
          null
      )
    ),
    description: stripHtml(
      ld?.description ||
        meta.contentByProperty.get("og:description") ||
        meta.contentByName.get("description") ||
        null
    ),
    canonicalUrl:
      absolutizeUrl(
        ld?.url ||
          extractLinkHref(html, "canonical", baseUrl) ||
          meta.contentByProperty.get("og:url") ||
          null,
        baseUrl
      ) || null,
    images: dedupeStrings([
      ...normalizeImageCollection(ld?.image, baseUrl),
      absolutizeUrl(meta.contentByProperty.get("og:image"), baseUrl),
      absolutizeUrl(meta.contentByName.get("twitter:image"), baseUrl),
    ].filter(Boolean)),
    externalId: normalizeOptionalString(ld?.productID || ld?.sku || null),
    price: extractPriceFromProductNode(ld),
  };
}

function extractAmazonCandidateData(html, baseUrl) {
  const meta = extractMetaTags(html);
  const ld = pickProductNode(extractJsonLdBlocks(html));
  const imageMap = extractJsonAssignment(html, [
    /"colorImages"\s*:\s*({[\s\S]*?})\s*,\s*"colorToAsin"/i,
    /"imageGalleryData"\s*:\s*(\[[\s\S]*?\])/i,
  ]);
  const images = [];

  if (imageMap?.initial || Array.isArray(imageMap)) {
    const source = Array.isArray(imageMap) ? imageMap : imageMap.initial;
    for (const image of source || []) {
      images.push(
        absolutizeUrl(image?.hiRes || image?.large || image?.mainUrl || image?.url || null, baseUrl)
      );
    }
  }

  const asin =
    normalizeOptionalString(meta.contentByName.get("asin")) ||
    normalizeOptionalString(meta.contentByName.get("ASIN")) ||
    normalizeOptionalString(
      String(html).match(/\b(?:asin|ASIN)\b["'=:\s]+([A-Z0-9]{10})/)?.[1] || null
    );

  const amount =
    normalizeOptionalString(ld?.offers?.price) ||
    normalizeOptionalString(
      String(html).match(/"priceAmount"\s*:\s*"([^"]+)"/i)?.[1] ||
      String(html).match(/\$([0-9]+(?:\.[0-9]{2})?)/)?.[1] ||
      null
    );

  return {
    title: normalizeWhitespace(
      decodeHtmlEntities(
        ld?.name ||
          meta.contentByProperty.get("og:title") ||
          meta.contentByName.get("title") ||
          null
      )
    ),
    description: stripHtml(
      ld?.description ||
        meta.contentByProperty.get("og:description") ||
        meta.contentByName.get("description") ||
        null
    ),
    canonicalUrl:
      absolutizeUrl(
        ld?.url ||
          extractLinkHref(html, "canonical", baseUrl) ||
          meta.contentByProperty.get("og:url") ||
          null,
        baseUrl
      ) || null,
    images: dedupeStrings([
      ...images.filter(Boolean),
      ...normalizeImageCollection(ld?.image, baseUrl),
      absolutizeUrl(meta.contentByProperty.get("og:image"), baseUrl),
      absolutizeUrl(meta.contentByName.get("twitter:image"), baseUrl),
    ].filter(Boolean)),
    externalId: asin || normalizeOptionalString(ld?.productID || ld?.sku || null),
    price: {
      amount,
      currency:
        normalizeOptionalString(ld?.offers?.priceCurrency) ||
        normalizeOptionalString(meta.contentByName.get("pricecurrency")) ||
        null,
    },
  };
}

function extractAliExpressCandidateData(html, baseUrl) {
  const meta = extractMetaTags(html);
  const ld = pickProductNode(extractJsonLdBlocks(html));
  const runParams = extractJsonAssignment(html, [
    /window\.__INITIAL_STATE__\s*=\s*({[\s\S]*?})\s*;\s*(?:<\/script>|window\.)/i,
    /window\.runParams\s*=\s*({[\s\S]*?})\s*;\s*(?:<\/script>|window\.)/i,
  ]);

  const subject =
    runParams?.data?.root?.fields ||
    runParams?.data ||
    runParams?.pageInfo ||
    runParams?.productInfo ||
    null;

  const gallery = subject?.imageModule?.imagePathList || subject?.imagePathList || [];
  const images = gallery.map((entry) => absolutizeUrl(entry, baseUrl));
  const amount =
    normalizeOptionalString(
      subject?.priceModule?.formatedActivityPrice ||
      subject?.priceModule?.formatedPrice ||
      subject?.priceModule?.minAmount?.value ||
      null
    ) ||
    extractPriceFromProductNode(ld).amount;
  const currency =
    normalizeOptionalString(
      subject?.priceModule?.currencyCode ||
      subject?.priceModule?.currency ||
      null
    ) || extractPriceFromProductNode(ld).currency;

  return {
    title: normalizeWhitespace(
      decodeHtmlEntities(
        subject?.titleModule?.subject ||
          ld?.name ||
          meta.contentByProperty.get("og:title") ||
          meta.contentByName.get("title") ||
          null
      )
    ),
    description: stripHtml(
      subject?.descriptionModule?.description ||
        ld?.description ||
        meta.contentByProperty.get("og:description") ||
        meta.contentByName.get("description") ||
        null
    ),
    canonicalUrl:
      absolutizeUrl(
        ld?.url ||
          extractLinkHref(html, "canonical", baseUrl) ||
          meta.contentByProperty.get("og:url") ||
          null,
        baseUrl
      ) || null,
    images: dedupeStrings([
      ...images.filter(Boolean),
      ...normalizeImageCollection(ld?.image, baseUrl),
      absolutizeUrl(meta.contentByProperty.get("og:image"), baseUrl),
      absolutizeUrl(meta.contentByName.get("twitter:image"), baseUrl),
    ].filter(Boolean)),
    externalId: normalizeOptionalString(
      subject?.productId || subject?.id || ld?.productID || ld?.sku || null
    ),
    price: {
      amount,
      currency,
    },
  };
}

function extractGenericCandidateData(html, baseUrl) {
  const meta = extractMetaTags(html);
  const jsonLdBlocks = extractJsonLdBlocks(html);
  const productNode = pickProductNode(jsonLdBlocks);

  return {
    title: normalizeWhitespace(
      decodeHtmlEntities(
        productNode?.name ||
          meta.contentByProperty.get("og:title") ||
          meta.contentByName.get("twitter:title") ||
          meta.contentByName.get("title") ||
          extractTitleTag(html)
      )
    ),
    description: stripHtml(
      productNode?.description ||
        meta.contentByProperty.get("og:description") ||
        meta.contentByName.get("twitter:description") ||
        meta.contentByName.get("description") ||
        null
    ),
    canonicalUrl:
      absolutizeUrl(
        productNode?.url ||
          extractLinkHref(html, "canonical", baseUrl) ||
          meta.contentByProperty.get("og:url") ||
          baseUrl,
        baseUrl
      ) || baseUrl,
    images: dedupeStrings([
      ...normalizeImageCollection(productNode?.image, baseUrl),
      absolutizeUrl(meta.contentByProperty.get("og:image"), baseUrl),
      absolutizeUrl(meta.contentByProperty.get("og:image:url"), baseUrl),
      absolutizeUrl(meta.contentByName.get("twitter:image"), baseUrl),
      absolutizeUrl(meta.contentByName.get("image"), baseUrl),
      ...extractImgTagCandidates(html, baseUrl),
    ].filter(Boolean)).slice(0, MAX_IMAGE_CANDIDATES),
    externalId: normalizeOptionalString(
      productNode?.productID ||
        productNode?.sku ||
        productNode?.mpn ||
        meta.contentByName.get("sku") ||
        null
    ),
    price: extractPriceFromProductNode(productNode),
    meta,
    productNode,
  };
}

function extractProviderCandidateData(provider, html, baseUrl) {
  if (provider === "etsy") {
    return extractEtsyCandidateData(html, baseUrl);
  }

  if (provider === "amazon") {
    return extractAmazonCandidateData(html, baseUrl);
  }

  if (provider === "aliexpress") {
    return extractAliExpressCandidateData(html, baseUrl);
  }

  return {};
}

function buildCandidateDataFromBrowserDom(domData, baseUrl) {
  const meta = {
    contentByProperty: new Map(
      Object.entries(domData?.metaByProperty || {}).map(([key, value]) => [key.toLowerCase(), value])
    ),
    contentByName: new Map(
      Object.entries(domData?.metaByName || {}).map(([key, value]) => [key.toLowerCase(), value])
    ),
  };

  return {
    title: normalizeWhitespace(
      decodeHtmlEntities(
        domData?.title ||
          meta.contentByProperty.get("og:title") ||
          meta.contentByName.get("twitter:title") ||
          null
      )
    ),
    description: stripHtml(
      domData?.description ||
        meta.contentByProperty.get("og:description") ||
        meta.contentByName.get("description") ||
        null
    ),
    canonicalUrl:
      absolutizeUrl(
        domData?.canonicalUrl ||
          meta.contentByProperty.get("og:url") ||
          null,
        baseUrl
      ) || null,
    images: dedupeStrings(
      (Array.isArray(domData?.images) ? domData.images : [])
        .map((entry) => absolutizeUrl(entry, baseUrl))
        .filter(Boolean)
    ).slice(0, MAX_IMAGE_CANDIDATES),
    externalId: normalizeOptionalString(domData?.externalId),
    price: {
      amount: normalizeOptionalString(domData?.priceAmount),
      currency: normalizeOptionalString(domData?.priceCurrency),
    },
  };
}

function mergeCandidateData(primary, secondary) {
  const primaryImages = Array.isArray(primary?.images) ? primary.images : [];
  const secondaryImages = Array.isArray(secondary?.images) ? secondary.images : [];
  const primaryPrice = primary?.price || {};
  const secondaryPrice = secondary?.price || {};

  return {
    title: primary?.title || secondary?.title || null,
    description: primary?.description || secondary?.description || null,
    canonicalUrl: primary?.canonicalUrl || secondary?.canonicalUrl || null,
    images: dedupeStrings([...primaryImages, ...secondaryImages]).slice(0, MAX_IMAGE_CANDIDATES),
    externalId: primary?.externalId || secondary?.externalId || null,
    price: {
      amount: primaryPrice.amount || secondaryPrice.amount || null,
      currency: primaryPrice.currency || secondaryPrice.currency || null,
    },
    meta: secondary?.meta || null,
    productNode: secondary?.productNode || null,
  };
}

function shouldTryBrowserFallback(reason, extractionErrors) {
  if (Array.isArray(extractionErrors) && extractionErrors.length > 0) {
    return true;
  }

  return ["captcha", "blocked", "forbidden", "challenge", "weak_extraction"].includes(
    String(reason || "").toLowerCase()
  );
}

async function loadPlaywright() {
  if (!playwrightModulePromise) {
    playwrightModulePromise = import("playwright");
  }

  return playwrightModulePromise;
}

export class ExternalProductImportService {
  constructor(options = {}) {
    this.timeoutMs = options.timeoutMs || DEFAULT_FETCH_TIMEOUT_MS;
    this.retryCount = Number.isInteger(options.retryCount)
      ? options.retryCount
      : DEFAULT_FETCH_RETRIES;
    this.browserTimeoutMs = options.browserTimeoutMs || DEFAULT_BROWSER_TIMEOUT_MS;
    this.httpClient = options.httpClient || axios.create({
      timeout: this.timeoutMs,
      maxRedirects: 5,
      responseType: "text",
      validateStatus: () => true,
      maxContentLength: MAX_BODY_LENGTH,
      maxBodyLength: MAX_BODY_LENGTH,
      decompress: true,
    });
  }

  async importFromUrl({ url, providerHint, sourceLabel }) {
    const normalizedUrl = normalizeOptionalString(url);
    if (!normalizedUrl) {
      throw this.buildInputError("url is required", {
        code: "invalid_url",
      });
    }

    let parsedUrl = null;

    try {
      parsedUrl = new URL(normalizedUrl);
    } catch {
      throw this.buildInputError("url must be a valid http or https URL", {
        code: "invalid_url",
      });
    }

    if (!["http:", "https:"].includes(parsedUrl.protocol)) {
      throw this.buildInputError("url must be a valid http or https URL", {
        code: "invalid_url",
      });
    }

    const normalizedProviderHint = normalizeOptionalString(providerHint)?.toLowerCase() || null;

    if (normalizedProviderHint && !SUPPORTED_PROVIDER_KEYS.has(normalizedProviderHint)) {
      throw this.buildInputError("provider_hint is not supported", {
        code: "provider_not_supported",
        provider: normalizedProviderHint,
        details: {
          supported_providers: Array.from(SUPPORTED_PROVIDER_KEYS),
        },
      });
    }

    const initialProvider = detectProvider(parsedUrl.hostname, normalizedProviderHint);
    const sourceLabelNormalized = normalizeOptionalString(sourceLabel);

    let httpResponse = null;
    let httpFetchError = null;

    try {
      httpResponse = await this.fetchExternalPage(parsedUrl.toString(), initialProvider);
    } catch (error) {
      if (error?.isExtractionError) {
        httpFetchError = error;
      } else {
        throw error;
      }
    }

    let chosenResponse = null;
    let chosenExtraction = null;
    let extractionErrors = [];
    let httpExtraction = null;

    if (httpResponse) {
      httpExtraction = this.extractNormalizedDataFromHtml({
        html: httpResponse.html,
        finalUrl: httpResponse.finalUrl,
        providerHint: initialProvider,
        sourceLabel: sourceLabelNormalized,
        strategy: httpResponse.strategy,
        attempts: httpResponse.attempts,
      });
      extractionErrors = validateExtractedProduct({
        title: httpExtraction.title,
        images: httpExtraction.images,
        canonicalUrl: httpExtraction.canonicalUrl,
      });

      if (extractionErrors.length === 0) {
        chosenResponse = httpResponse;
        chosenExtraction = httpExtraction;
      }
    }

    const shouldUseBrowserFallback =
      !chosenExtraction &&
      (httpFetchError
        ? shouldTryBrowserFallback(httpFetchError.reason, [])
        : shouldTryBrowserFallback("weak_extraction", extractionErrors));

    let browserFailure = null;

    if (shouldUseBrowserFallback) {
      try {
        const browserResponse = await this.fetchExternalPageWithBrowser(
          httpResponse?.finalUrl || parsedUrl.toString(),
          initialProvider,
          httpFetchError?.attempts || httpResponse?.attempts || []
        );
        const browserExtraction = this.extractNormalizedDataFromBrowser({
          html: browserResponse.html,
          domData: browserResponse.domData,
          finalUrl: browserResponse.finalUrl,
          providerHint: initialProvider,
          sourceLabel: sourceLabelNormalized,
          strategy: browserResponse.strategy,
          attempts: browserResponse.attempts,
        });
        const browserErrors = validateExtractedProduct({
          title: browserExtraction.title,
          images: browserExtraction.images,
          canonicalUrl: browserExtraction.canonicalUrl,
        });

        if (browserErrors.length === 0) {
          chosenResponse = browserResponse;
          chosenExtraction = browserExtraction;
          extractionErrors = [];
        } else {
          browserFailure = this.buildExtractionError(
            "Browser fallback loaded the page but extraction remained too weak",
            {
              reason: "weak_extraction",
              provider: initialProvider,
              attempts: [
                ...(browserResponse.attempts || []),
                {
                  method: "playwright_extract",
                  client: "playwright",
                  reason: "weak_extraction",
                  details: browserErrors,
                },
              ],
            }
          );
          extractionErrors = browserErrors;
        }
      } catch (error) {
        if (error?.isExtractionError) {
          browserFailure = error;
        } else {
          throw error;
        }
      }
    }

    if (!chosenExtraction) {
      if (browserFailure) {
        throw browserFailure;
      }

      if (httpFetchError) {
        throw httpFetchError;
      }

      throw this.buildExtractionError(
        "External product page loaded but did not contain enough metadata",
        {
          reason: "weak_extraction",
          provider: initialProvider,
          attempts: [
            ...(httpResponse?.attempts || []),
            {
              method: "http_extract",
              client: "http",
              reason: "weak_extraction",
              details: extractionErrors,
            },
          ],
        }
      );
    }

    return this.buildImportResult(chosenExtraction);
  }

  extractNormalizedDataFromHtml({
    html,
    finalUrl,
    providerHint,
    sourceLabel,
    strategy,
    attempts,
  }) {
    const finalParsedUrl = new URL(finalUrl);
    const provider = detectProvider(finalParsedUrl.hostname, providerHint);
    const generic = extractGenericCandidateData(html, finalUrl);
    const providerSpecific = extractProviderCandidateData(provider, html, finalUrl);
    const extracted = mergeCandidateData(providerSpecific, generic);

    return this.buildNormalizedExtractionResult({
      provider,
      sourceLabel,
      finalUrl,
      html,
      title: extracted.title,
      description: extracted.description,
      canonicalUrl: extracted.canonicalUrl || finalUrl,
      images: extracted.images,
      externalProductId: extracted.externalId,
      priceAmount: extracted.price?.amount || null,
      priceCurrency: extracted.price?.currency || null,
      strategy,
      attempts,
      genericMeta: generic.meta,
      genericProductNode: generic.productNode,
    });
  }

  extractNormalizedDataFromBrowser({
    html,
    domData,
    finalUrl,
    providerHint,
    sourceLabel,
    strategy,
    attempts,
  }) {
    const finalParsedUrl = new URL(finalUrl);
    const provider = detectProvider(finalParsedUrl.hostname, providerHint);
    const generic = extractGenericCandidateData(html, finalUrl);
    const providerSpecific = extractProviderCandidateData(provider, html, finalUrl);
    const browserDom = buildCandidateDataFromBrowserDom(domData, finalUrl);
    const extracted = mergeCandidateData(
      browserDom,
      mergeCandidateData(providerSpecific, generic)
    );

    return this.buildNormalizedExtractionResult({
      provider,
      sourceLabel,
      finalUrl,
      html,
      title: extracted.title,
      description: extracted.description,
      canonicalUrl: extracted.canonicalUrl || finalUrl,
      images: extracted.images,
      externalProductId: extracted.externalId,
      priceAmount: extracted.price?.amount || null,
      priceCurrency: extracted.price?.currency || null,
      strategy,
      attempts,
      genericMeta: generic.meta,
      genericProductNode: generic.productNode,
      browserDomData: domData,
    });
  }

  buildNormalizedExtractionResult({
    provider,
    sourceLabel,
    finalUrl,
    html,
    title,
    description,
    canonicalUrl,
    images,
    externalProductId,
    priceAmount,
    priceCurrency,
    strategy,
    attempts,
    genericMeta,
    genericProductNode,
    browserDomData,
  }) {
    const sourceDomain = new URL(finalUrl).hostname.toLowerCase();
    const caption = buildCaption(title, description, priceAmount);
    const generatedProductId = externalProductId || buildGeneratedExternalProductId(canonicalUrl);

    return {
      provider,
      sourceDomain,
      title,
      description,
      canonicalUrl,
      images,
      externalProductId,
      priceAmount,
      priceCurrency,
      normalizedPayload: {
        product_id: generatedProductId,
        url: canonicalUrl,
        images,
        last_image: images[0] || null,
        last_title: title || null,
        last_description: description || title || null,
        last_caption: caption,
        status: "scheduled",
        next_run_at: new Date().toISOString(),
        publish_count: 0,
        max_republish: null,
        workflow_source: "external_url_import",
      },
      preview: buildPreview({
        title,
        description,
        images,
        canonicalUrl,
        provider,
        sourceDomain,
        externalProductId,
        priceAmount,
        priceCurrency,
        metadata: {
          source_label: sourceLabel,
          final_url: finalUrl,
          provider_detected: provider,
          fetch_strategy: strategy,
          extraction_signals: {
            json_ld_product: Boolean(genericProductNode),
            open_graph_title: Boolean(genericMeta?.contentByProperty.get("og:title")),
            open_graph_description: Boolean(genericMeta?.contentByProperty.get("og:description")),
            open_graph_image: Boolean(genericMeta?.contentByProperty.get("og:image")),
            canonical_link: Boolean(extractLinkHref(html, "canonical", finalUrl)),
            title_tag: Boolean(extractTitleTag(html)),
            image_count: images.length,
            browser_dom_used: Boolean(browserDomData),
          },
          fetch_attempts: attempts || [],
          browser_dom_snapshot: browserDomData
            ? {
                title: browserDomData.title || null,
                canonical_url: browserDomData.canonicalUrl || null,
                image_count: Array.isArray(browserDomData.images) ? browserDomData.images.length : 0,
              }
            : null,
        },
      }),
    };
  }

  buildImportResult(extraction) {
    return {
      normalizedPayload: extraction.normalizedPayload,
      preview: extraction.preview,
      validation: {
        can_create_queue_item: true,
        errors: [],
      },
    };
  }

  async fetchExternalPage(url, provider) {
    const attempts = [];
    const fetchAttempts = buildFetchAttempts(url, provider);

    for (let attemptIndex = 0; attemptIndex <= this.retryCount; attemptIndex += 1) {
      for (const fetchAttempt of fetchAttempts) {
        const attemptResult =
          fetchAttempt.client === "fetch"
            ? await this.runFetchAttempt(url, fetchAttempt, attemptIndex)
            : await this.runAxiosAttempt(url, fetchAttempt, attemptIndex);

        attempts.push(attemptResult.attempt);

        if (attemptResult.success) {
          return {
            html: attemptResult.html,
            finalUrl: attemptResult.finalUrl,
            contentType: attemptResult.contentType,
            strategy: attemptResult.attempt.method,
            attempts,
          };
        }
      }
    }

    const failure = buildFailureReport(attempts);
    throw this.buildExtractionError(failure.message, {
      reason: failure.reason,
      provider,
      attempts: failure.attempts,
    });
  }

  async fetchExternalPageWithBrowser(url, provider, priorAttempts = []) {
    let playwright = null;

    try {
      playwright = await loadPlaywright();
    } catch (error) {
      throw this.buildExtractionError("Playwright browser fallback is unavailable on this server", {
        reason: "browser_unavailable",
        provider,
        details: {
          dependency: "playwright",
        },
        attempts: [
          ...priorAttempts,
          {
            method: "playwright_import",
            client: "playwright",
            reason: "browser_unavailable",
            error: normalizeOptionalString(error?.message),
          },
        ],
      });
    }

    const headers = buildRequestHeaders(url, provider, DESKTOP_USER_AGENT);
    let browser = null;
    let context = null;
    let page = null;

    try {
      browser = await playwright.chromium.launch({
        headless: true,
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
      });
      context = await browser.newContext({
        userAgent: DESKTOP_USER_AGENT,
        locale: "en-US",
        viewport: { width: 1440, height: 1200 },
        extraHTTPHeaders: headers,
      });
      page = await context.newPage();
      page.setDefaultNavigationTimeout(this.browserTimeoutMs);
      page.setDefaultTimeout(this.browserTimeoutMs);

      const response = await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: this.browserTimeoutMs,
      });

      await this.waitForProviderReady(page, provider);
      await page.waitForTimeout(DEFAULT_BROWSER_WAIT_AFTER_GOTO_MS);

      const html = await page.content();
      const finalUrl = page.url() || url;
      const status = response?.status() ?? null;
      const statusText = normalizeOptionalString(response?.statusText?.());
      const blockedReason = detectBlockedReason(html, status);
      const domData = await this.extractBrowserDomData(page, provider);

      const attempts = [
        ...priorAttempts,
        {
          method: "playwright_browser",
          client: "playwright",
          status,
          status_text: statusText,
          final_url: finalUrl,
          blocked_reason: blockedReason,
        },
      ];

      if (blockedReason) {
        const failure = buildFailureReport(attempts);
        throw this.buildExtractionError(failure.message, {
          reason: failure.reason,
          provider,
          attempts,
        });
      }

      if (!normalizeOptionalString(html)) {
        throw this.buildExtractionError("Playwright browser fallback returned an empty page", {
          reason: "browser_failed",
          provider,
          attempts,
        });
      }

      return {
        html,
        domData,
        finalUrl,
        strategy: "playwright_browser",
        attempts,
      };
    } catch (error) {
      if (error?.isExtractionError) {
        throw error;
      }

      const errorMessage = normalizeOptionalString(error?.message) || "";
      const unavailable =
        /error while loading shared libraries/i.test(errorMessage) ||
        /cannot open shared object file/i.test(errorMessage) ||
        /executable doesn't exist/i.test(errorMessage) ||
        /host system is missing dependencies/i.test(errorMessage);

      throw this.buildExtractionError("Playwright browser fallback failed to load the page", {
        reason: unavailable ? "browser_unavailable" : "browser_failed",
        provider,
        attempts: [
          ...priorAttempts,
          {
            method: "playwright_browser",
            client: "playwright",
            reason: unavailable ? "browser_unavailable" : "browser_failed",
            error: errorMessage,
          },
        ],
      });
    } finally {
      try {
        await page?.close();
      } catch {}
      try {
        await context?.close();
      } catch {}
      try {
        await browser?.close();
      } catch {}
    }
  }

  async waitForProviderReady(page, provider) {
    const selectors = PROVIDER_BROWSER_SELECTORS[provider]?.waitFor || [];

    for (const selector of selectors) {
      try {
        await page.waitForSelector(selector, { timeout: 3000, state: "attached" });
        return;
      } catch {}
    }
  }

  async extractBrowserDomData(page, provider) {
    const selectors = PROVIDER_BROWSER_SELECTORS[provider] || {};

    return page.evaluate(
      ({ provider, selectors, maxImages }) => {
        const pickText = (selectorList = []) => {
          for (const selector of selectorList) {
            const element = document.querySelector(selector);
            if (!element) {
              continue;
            }

            const content =
              element.getAttribute("content") ||
              element.textContent ||
              element.getAttribute("value") ||
              "";
            const normalized = String(content).replace(/\s+/g, " ").trim();
            if (normalized) {
              return normalized;
            }
          }

          return null;
        };

        const pickImages = (selectorList = []) => {
          const values = [];
          const pushImage = (candidate) => {
            const normalized = String(candidate || "").trim();
            if (!normalized || values.includes(normalized)) {
              return;
            }

            values.push(normalized);
          };

          for (const selector of selectorList) {
            const elements = Array.from(document.querySelectorAll(selector));
            for (const element of elements) {
              pushImage(
                element.getAttribute("src") ||
                  element.getAttribute("data-src") ||
                  element.getAttribute("data-old-hires") ||
                  element.getAttribute("data-zoom-image") ||
                  element.currentSrc
              );
              if (values.length >= maxImages) {
                return values;
              }
            }
          }

          const genericElements = Array.from(document.images || []);
          for (const element of genericElements) {
            pushImage(
              element.getAttribute("src") ||
                element.getAttribute("data-src") ||
                element.currentSrc
            );
            if (values.length >= maxImages) {
              break;
            }
          }

          return values;
        };

        const metaByProperty = {};
        const metaByName = {};
        for (const tag of Array.from(document.querySelectorAll("meta"))) {
          const content = String(tag.getAttribute("content") || "").trim();
          if (!content) {
            continue;
          }

          const property = String(tag.getAttribute("property") || "").trim().toLowerCase();
          const name = String(tag.getAttribute("name") || "").trim().toLowerCase();

          if (property) {
            metaByProperty[property] = content;
          }

          if (name) {
            metaByName[name] = content;
          }
        }

        const canonicalUrl =
          document.querySelector('link[rel="canonical"]')?.getAttribute("href") ||
          metaByProperty["og:url"] ||
          null;

        const amazonAsin =
          metaByName["asin"] ||
          document.querySelector("#ASIN")?.getAttribute("value") ||
          null;

        return {
          provider,
          title:
            pickText(selectors.title) ||
            metaByProperty["og:title"] ||
            document.title ||
            null,
          description:
            pickText(selectors.description) ||
            metaByProperty["og:description"] ||
            metaByName["description"] ||
            null,
          canonicalUrl,
          images: pickImages(selectors.image),
          priceAmount: pickText(selectors.price),
          priceCurrency:
            metaByName["pricecurrency"] ||
            metaByName["twitter:data1"] ||
            null,
          externalId:
            amazonAsin ||
            metaByName["sku"] ||
            document.querySelector('[data-product-id]')?.getAttribute("data-product-id") ||
            null,
          metaByProperty,
          metaByName,
        };
      },
      {
        provider,
        selectors,
        maxImages: MAX_IMAGE_CANDIDATES,
      }
    );
  }

  async runAxiosAttempt(url, fetchAttempt, attemptIndex) {
    try {
      const response = await this.httpClient.get(url, {
        headers: fetchAttempt.headers,
      });
      const html = typeof response.data === "string" ? response.data : String(response.data || "");
      const finalUrl = response?.request?.res?.responseUrl || response?.config?.url || url;
      const contentType = normalizeOptionalString(response.headers?.["content-type"]);
      const blockedReason = detectBlockedReason(html, response.status);
      const attempt = {
        method: fetchAttempt.label,
        client: "axios",
        retry_index: attemptIndex,
        status: response.status,
        status_text: normalizeOptionalString(response.statusText),
        final_url: finalUrl,
        content_type: contentType,
        blocked_reason: blockedReason,
      };

      if (response.status >= 200 && response.status < 400 && html.trim() && !blockedReason) {
        return {
          success: true,
          html,
          finalUrl,
          contentType,
          attempt,
        };
      }

      return {
        success: false,
        attempt,
      };
    } catch (error) {
      const attempt = {
        method: fetchAttempt.label,
        client: "axios",
        retry_index: attemptIndex,
        reason:
          error?.code === "ECONNABORTED" || /timeout/i.test(String(error?.message || ""))
            ? "timeout"
            : "network_error",
        error: normalizeOptionalString(error?.message),
      };

      return {
        success: false,
        attempt,
      };
    }
  }

  async runFetchAttempt(url, fetchAttempt, attemptIndex) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: fetchAttempt.headers,
        redirect: "follow",
        compress: true,
        size: MAX_BODY_LENGTH,
        timeout: this.timeoutMs,
      });
      const html = await response.text();
      const finalUrl = response.url || url;
      const contentType = normalizeOptionalString(response.headers.get("content-type"));
      const blockedReason = detectBlockedReason(html, response.status);
      const attempt = {
        method: fetchAttempt.label,
        client: "fetch",
        retry_index: attemptIndex,
        status: response.status,
        status_text: normalizeOptionalString(response.statusText),
        final_url: finalUrl,
        content_type: contentType,
        blocked_reason: blockedReason,
      };

      if (response.ok && html.trim() && !blockedReason) {
        return {
          success: true,
          html,
          finalUrl,
          contentType,
          attempt,
        };
      }

      return {
        success: false,
        attempt,
      };
    } catch (error) {
      const attempt = {
        method: fetchAttempt.label,
        client: "fetch",
        retry_index: attemptIndex,
        reason:
          error?.type === "request-timeout" || /timeout/i.test(String(error?.message || ""))
            ? "timeout"
            : "network_error",
        error: normalizeOptionalString(error?.message),
      };

      return {
        success: false,
        attempt,
      };
    }
  }

  buildInputError(message, details = {}) {
    const error = new Error(message);
    error.isInputError = true;
    error.code = details.code || null;
    error.provider = details.provider || null;
    error.details = details.details || null;
    return error;
  }

  buildExtractionError(message, details = {}) {
    const error = new Error(message);
    error.isExtractionError = true;
    error.reason = details.reason || null;
    error.provider = details.provider || null;
    error.details = details.details || null;
    error.attempts = Array.isArray(details.attempts) ? details.attempts : [];
    return error;
  }
}

export default ExternalProductImportService;
