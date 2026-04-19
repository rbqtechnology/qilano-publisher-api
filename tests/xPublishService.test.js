import test from "node:test";
import assert from "node:assert/strict";
import {
  buildXCreateTweetBody,
  buildXPostText,
  publishToX,
} from "../services/xPublishService.js";

test("builds x post text from x.text first and appends the link once", () => {
  assert.equal(
    buildXPostText({
      x_text: "Fresh drop",
      tweet_text: "tweet fallback",
      post_text: "post fallback",
      x_link: "https://qilano.test/products/42",
    }),
    "Fresh drop\n\nhttps://qilano.test/products/42"
  );
});

test("publishes an x text-only post", async () => {
  const calls = [];
  const httpClient = {
    async post(url, body) {
      calls.push({ url, body });
      return {
        data: {
          data: {
            id: "111",
          },
        },
      };
    },
  };

  const result = await publishToX(
    {
      x_text: "Fresh drop",
      x_link: "https://qilano.test/products/42",
      normalized_x_media_urls: [],
    },
    {
      id: 17,
      handle: "qilano_x",
      access_token: "secret",
    },
    {
      httpClient,
      now: () => new Date("2026-04-19T00:00:00.000Z"),
      config: { apiBaseUrl: "https://api.test" },
    }
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://api.test/2/tweets");
  assert.deepEqual(calls[0].body, {
    text: "Fresh drop\n\nhttps://qilano.test/products/42",
  });
  assert.deepEqual(result.publish_result, {
    external_id: "111",
    published_at: "2026-04-19T00:00:00.000Z",
    url: "https://x.com/qilano_x/status/111",
    text: "Fresh drop\n\nhttps://qilano.test/products/42",
  });
});

test("publishes an x post with uploaded media ids", async () => {
  const posts = [];
  const gets = [];
  const httpClient = {
    async get(url) {
      gets.push(url);
      return {
        data: Buffer.from("fake-image"),
        headers: {
          "content-type": "image/jpeg",
        },
      };
    },
    async post(url, body) {
      posts.push({ url, body });
      if (url === "https://upload.twitter.test/1.1/media/upload.json") {
        return {
          data: {
            media_id_string: String(posts.length),
          },
        };
      }

      return {
        data: {
          data: {
            id: "222",
          },
        },
      };
    },
  };

  const result = await publishToX(
    {
      tweet_text: "Fresh drop",
      normalized_x_media_urls: [
        "https://cdn.qilano.test/product-1.jpg",
        "https://cdn.qilano.test/product-2.jpg",
      ],
    },
    {
      id: 17,
      handle: "qilano_x",
      access_token: "secret",
    },
    {
      httpClient,
      now: () => new Date("2026-04-19T00:00:00.000Z"),
      config: {
        apiBaseUrl: "https://api.test",
        uploadBaseUrl: "https://upload.twitter.test/1.1",
      },
    }
  );

  assert.deepEqual(gets, [
    "https://cdn.qilano.test/product-1.jpg",
    "https://cdn.qilano.test/product-2.jpg",
  ]);
  assert.deepEqual(buildXCreateTweetBody({ tweet_text: "Fresh drop" }, ["1", "2"]), {
    text: "Fresh drop",
    media: {
      media_ids: ["1", "2"],
    },
  });
  assert.equal(posts[2].url, "https://api.test/2/tweets");
  assert.deepEqual(posts[2].body, {
    text: "Fresh drop",
    media: {
      media_ids: ["1", "2"],
    },
  });
  assert.equal(result.publish_result.external_id, "222");
});
