import test from "node:test";
import assert from "node:assert/strict";
import { attachNormalizedPublishResult } from "../services/publishResult.js";

test("adds a normalized publish result for instagram success without changing publish_result", () => {
  const response = attachNormalizedPublishResult({
    success: true,
    platform: "instagram",
    social_account_id: 9,
    publish_result: {
      external_id: "123",
      url: "https://www.instagram.com/p/abc123/",
      published_at: "2026-04-19T00:00:00.000Z",
      caption: "Fresh drop",
    },
    error: null,
  });

  assert.equal(response.publish_result.caption, "Fresh drop");
  assert.deepEqual(response.normalized_publish_result, {
    success: true,
    platform: "instagram",
    external_id: "123",
    url: "https://www.instagram.com/p/abc123/",
    published_at: "2026-04-19T00:00:00.000Z",
    message: "instagram publish succeeded",
  });
});

test("adds a normalized publish result for x failures", () => {
  const response = attachNormalizedPublishResult({
    success: false,
    platform: "x",
    social_account_id: 17,
    publish_result: null,
    error: {
      message: "X social account is missing access_token",
      code: "ACCESS_TOKEN_MISSING",
      type: "account_error",
    },
  });

  assert.equal(response.message, "X social account is missing access_token");
  assert.deepEqual(response.normalized_publish_result, {
    success: false,
    platform: "x",
    external_id: null,
    url: null,
    published_at: null,
    message: "X social account is missing access_token",
  });
});
