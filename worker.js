
// /home/ubuntu/publisher-api/worker.js



import dotenv from "dotenv";



dotenv.config({ quiet: true, path: "/home/ubuntu/publisher-api/.env" });



const fetch = (...args) =>

  import("node-fetch").then(({ default: f }) => f(...args));



// ========================

// Config

// ========================



const API_BASE = process.env.API_BASE || "https://api.qilano.com";

const WORKER_TOKEN = process.env.WORKER_TOKEN || "supersecretworker";

const WORKER_ID = process.env.WORKER_ID || "w1";



const LEASE_SECONDS = Number(process.env.LEASE_SECONDS || 60);

const HEARTBEAT_EVERY_MS = Number(process.env.HEARTBEAT_EVERY_MS || 20000);

const IDLE_SLEEP_MS = Number(process.env.IDLE_SLEEP_MS || 3000);

const RECOVER_EVERY_MS = Number(process.env.RECOVER_EVERY_MS || 60000);



// Backoff when no jobs available

const BACKOFF_MIN_MS = Number(process.env.BACKOFF_MIN_MS || IDLE_SLEEP_MS || 3000);

const BACKOFF_MAX_MS = Number(process.env.BACKOFF_MAX_MS || 60000);



// ========================

// HTTP helper

// ========================



async function workerPost2(path, body) {

  const r = await fetch(`${API_BASE}${path}`, {

    method: "POST",

    headers: {

      "x-worker-token": WORKER_TOKEN,

      "Content-Type": "application/json",

      // If your API uses Bearer token instead, uncomment:

      // "Authorization": "Bearer " + WORKER_TOKEN,

    },

    body: JSON.stringify(body ?? {}),

  });



  const json = await r.json().catch(() => ({}));

  return { status: r.status, json };

}



// ========================

// Heartbeat

// ========================



function startHeartbeat(id) {

  return setInterval(() => {

    workerPost2(`/worker/heartbeat/${id}`, {

      worker_id: WORKER_ID,

      lease_seconds: LEASE_SECONDS,

    }).catch(() => {});

  }, HEARTBEAT_EVERY_MS);

}



// ========================

// Your publish logic (replace this)

// ========================



async function processItem(item) {

  const id = Number(item?.id);

  if (!Number.isFinite(id)) {

    throw new Error("Invalid queue item id");

  }

  const result = await workerPost2(`/internal/repost/${id}/publish`, {

    worker_id: WORKER_ID,

    dry_run: false,

  });

  if ((result?.status ?? 0) >= 400) {

    throw new Error(result?.json?.message || result?.json?.error || "Internal publish request failed");

  }

  const publishResponse = result?.json?.publish_result ?? null;
  const externalId = String(
    publishResponse?.publish_result?.external_id || ""
  ).trim();
  const permalink = String(
    publishResponse?.publish_result?.url || ""
  ).trim();
  const publishedAt = String(
    publishResponse?.publish_result?.published_at || ""
  ).trim();
  const publishVerified =
    publishResponse?.success === true &&
    externalId &&
    publishedAt &&
    /https?:\/\/(www\.)?instagram\.com\//i.test(permalink);

  return {
    ok: publishVerified,
    publishResponse,
    statusUpdateApplied: result?.json?.status_update?.ok === true,
    message: result?.json?.message || null,
  };

}



// ========================

// Log helpers

// ========================



let lastNoJobLogAt = 0;



function shouldLogNoJobs(intervalMs = 60_000) {

  const now = Date.now();

  if (now - lastNoJobLogAt >= intervalMs) {

    lastNoJobLogAt = now;

    return true;

  }

  return false;

}



// ========================

// Work cycle

// ========================



async function runOnce() {

  const claim = await workerPost2("/worker/claim", {

    worker_id: WORKER_ID,

    lease_seconds: LEASE_SECONDS,

  });



  const claimJson = claim?.json ?? {};



  // Any HTTP error from claim

  if ((claim?.status ?? 0) >= 400) {

    console.log("[worker] claim failed", claim?.status, claimJson);

    return { didWork: false, backoff: true };

  }



  // Support either {item:...} or {job:...}

  const item = claimJson?.item ?? claimJson?.job ?? null;



  if (!item) {

    // ✅ FIX: NO ELSE LOGGING (prevents spam)

    if (shouldLogNoJobs(60_000)) {

      const hint = claimJson?.message || claimJson?.error || null;

      if (hint) console.log("[worker] no jobs:", hint);

      else console.log("[worker] no jobs available");

    }

    return { didWork: false, backoff: true };

  }



  const id = item.id;

  console.log("[worker] claimed id=", id);



  const hb = startHeartbeat(id);



  try {

    console.log("[worker] publish start", {
      id,
      product_id: item.product_id,
      status: item.status,
      publish_count: item.publish_count ?? null,
      next_run_at: item.next_run_at ?? null,
    });

    const publishOutcome = await processItem(item);
    const ok = publishOutcome?.ok === true;



    if (ok) {

      if (publishOutcome.statusUpdateApplied) {

        console.log("[worker] verified publish success", {
          id,
          product_id: item.product_id,
          external_id: publishOutcome.publishResponse?.publish_result?.external_id ?? null,
          permalink: publishOutcome.publishResponse?.publish_result?.url ?? null,
        });

      } else {

        const res = await workerPost2(`/worker/succeed/${id}`, {

          worker_id: WORKER_ID,

          publish_response: publishOutcome.publishResponse,

        });

        if ((res?.status ?? 0) >= 400) {

          console.log("[worker] succeed failed", res?.status, res?.json);

        } else {

          console.log("[worker] publish success", {
            id,
            product_id: item.product_id,
            status: res?.json?.item?.status ?? null,
            publish_count: res?.json?.item?.publish_count ?? null,
            next_run_at: res?.json?.item?.next_run_at ?? null,
            external_id: publishOutcome.publishResponse?.publish_result?.external_id ?? null,
            permalink: publishOutcome.publishResponse?.publish_result?.url ?? null,
          });

        }

      }

    } else {

      if (publishOutcome?.statusUpdateApplied) {

        console.log("[worker] publish failure already recorded", {
          id,
          product_id: item.product_id,
          message: publishOutcome?.message ?? null,
        });

      } else {

        const res = await workerPost2(`/worker/fail/${id}`, {

          worker_id: WORKER_ID,

          error: publishOutcome?.message || "publish failed",

        });

        if ((res?.status ?? 0) >= 400) {

          console.log("[worker] fail endpoint failed", res?.status, res?.json);

        } else {

          console.log("[worker] fail id=", id);

        }

      }
    }

  } catch (e) {

    console.log("[worker] exception id=", id, e?.message || e);



    const res = await workerPost2(`/worker/fail/${id}`, {

      worker_id: WORKER_ID,

      error: String(e?.message || e),

    });



    if ((res?.status ?? 0) >= 400) {

      console.log("[worker] fail endpoint failed", res?.status, res?.json);

    }

  } finally {

    clearInterval(hb);

    console.log("[worker] done id=", id);

  }



  return { didWork: true, backoff: false };

}



// ========================

// Recover stuck jobs periodically (optional but useful)

// ========================



let lastRecoverAt = 0;



async function maybeRecoverStuck() {

  const now = Date.now();

  if (now - lastRecoverAt < RECOVER_EVERY_MS) return;

  lastRecoverAt = now;



  try {

    const res = await workerPost2("/worker/recover-stuck", {});

    // Quiet mode: log only on error

    if ((res?.status ?? 0) >= 400) {

      console.log("[worker] recover-stuck failed", res?.status, res?.json);

    }

  } catch (e) {

    console.log("[worker] recover-stuck error", e?.message || e);

  }

}



async function sleep(ms) {

  return new Promise((r) => setTimeout(r, ms));

}



// ========================

// Main

// ========================



async function main() {

  let backoffMs = BACKOFF_MIN_MS;



  while (true) {

    await maybeRecoverStuck();



    try {

      const { didWork, backoff } = await runOnce();



      if (didWork) {

        backoffMs = BACKOFF_MIN_MS; // reset after success

        continue;

      }



      if (backoff) {

        await sleep(backoffMs);

        backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);

      } else {

        await sleep(IDLE_SLEEP_MS);

      }

    } catch (e) {

      console.error("[worker] runOnce error", e?.message || e);

      await sleep(Math.min(Math.max(IDLE_SLEEP_MS, 1000), 5000));

    }

  }

}



main().catch((e) => {

  console.error("[worker fatal]", e);

  process.exit(1);

});
