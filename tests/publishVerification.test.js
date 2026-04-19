import test from "node:test";
import assert from "node:assert/strict";
import { normalizeConfirmedPublishResponse } from "../services/publishVerification.js";

test("accepts the existing instagram publish shape", () => {
  const result = normalizeConfirmedPublishResponse({
    success: true,
    platform: "instagram",
    publish_result: {
      external_id: "123",
      url: "https://www.instagram.com/p/abc123/",
      published_at: "2026-04-19T00:00:00.000Z",
    },
  });

  assert.deepEqual(result, {
    platform: "instagram",
    externalId: "123",
    url: "https://www.instagram.com/p/abc123/",
    publishedAt: "2026-04-19T00:00:00.000Z",
  });
});

test("accepts x publish verification", () => {
  const result = normalizeConfirmedPublishResponse({
    success: true,
    platform: "x",
    publish_result: {
      external_id: "987654321",
      url: "https://x.com/qilano/status/987654321",
      published_at: "2026-04-19T00:00:00.000Z",
    },
  });

  assert.deepEqual(result, {
    platform: "x",
    externalId: "987654321",
    url: "https://x.com/qilano/status/987654321",
    publishedAt: "2026-04-19T00:00:00.000Z",
  });
});

test("rejects mismatched x publish urls", () => {
  const result = normalizeConfirmedPublishResponse({
    success: true,
    platform: "x",
    publish_result: {
      external_id: "987654321",
      url: "https://example.com/not-an-x-url",
      published_at: "2026-04-19T00:00:00.000Z",
    },
  });

  assert.equal(result, null);
});
