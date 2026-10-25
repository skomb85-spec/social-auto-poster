// social-auto-poster — Phase 1 (ruri Threads pilot)
//
// Runs on a Cloudflare Cron Trigger (every 30 min). Reads the approved
// queue from this repo's queue.json (raw URL), posts due items to the
// Threads API with the long-lived token stored in Workers KV, and
// records per-item results back into KV. No secrets live in this file.
//
// KV binding: STATE (namespace "auto-poster")
//   token:threads:ruri   -> long-lived Threads token (user-saved)
//   result:<item_id>     -> per-item result JSON (double-post guard)
//   state:last_run       -> summary of the most recent scheduled run
//
// HTTP endpoints (read-only, for verification):
//   /           -> service banner
//   /__status   -> last scheduled run summary from KV
//   /__dryrun   -> token whoami + queue read + due judgement, no posting

const CONFIG = {
  queueUrl:
    "https://raw.githubusercontent.com/skomb85-spec/social-auto-poster/main/queue.json",
  tokenKey: "token:threads:ruri",
  account: "ruri",
  platform: "threads",
  graph: "https://graph.threads.net/v1.0",
  maxAttempts: 3,
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class RateLimitError extends Error {}

async function threadsFetch(path, token, options = {}) {
  const resp = await fetch(CONFIG.graph + path, {
    ...options,
    headers: { Authorization: `Bearer ${token}`, ...(options.headers || {}) },
  });
  if (resp.status === 429) throw new RateLimitError("threads http 429");
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg =
      (data && data.error && (data.error.message || data.error.type)) ||
      `http ${resp.status}`;
    if (data && data.error && data.error.code === 4) {
      throw new RateLimitError("threads error code 4 (rate limit)");
    }
    throw new Error(`threads api: ${msg}`);
  }
  return data;
}

async function threadsPostForm(path, token, params) {
  const body = new URLSearchParams(params);
  return threadsFetch(path, token, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
}

async function postToThreads(me, token, text) {
  const container = await threadsPostForm(`/${me.id}/threads`, token, {
    media_type: "TEXT",
    text,
  });
  // Text containers are usually ready within seconds; poll briefly.
  let status = "IN_PROGRESS";
  for (let i = 0; i < 5 && status === "IN_PROGRESS"; i++) {
    await sleep(4000);
    const s = await threadsFetch(
      `/${container.id}?fields=status,error_message`,
      token
    );
    status = s.status || "FINISHED";
    if (status === "ERROR") {
      throw new Error(`container error: ${s.error_message || "unknown"}`);
    }
  }
  if (status === "EXPIRED") throw new Error("container expired");
  const published = await threadsPostForm(`/${me.id}/threads_publish`, token, {
    creation_id: container.id,
  });
  let permalink = null;
  try {
    const p = await threadsFetch(`/${published.id}?fields=permalink`, token);
    permalink = p.permalink || null;
  } catch (_) {
    // permalink is best-effort; the post itself already succeeded
  }
  return { postId: published.id, permalink };
}

async function run(env, { dry = false } = {}) {
  const startedAt = new Date().toISOString();
  const summary = {
    at: startedAt,
    dry,
    username: null,
    queue_items: 0,
    due: [],
    posted: [],
    skipped: [],
    errors: [],
  };

  const token = await env.STATE.get(CONFIG.tokenKey);
  if (!token) {
    summary.errors.push(`kv token missing: ${CONFIG.tokenKey}`);
    return summary;
  }

  // Whoami: validates the token on every run and resolves the user id.
  const me = await threadsFetch("/me?fields=id,username", token);
  summary.username = me.username || null;

  const qResp = await fetch(`${CONFIG.queueUrl}?t=${Date.now()}`, {
    headers: { "User-Agent": "social-auto-poster" },
    cf: { cacheTtl: 0, cacheEverything: false },
  });
  if (!qResp.ok) {
    summary.errors.push(`queue fetch http ${qResp.status}`);
    return summary;
  }
  const queue = await qResp.json();
  const items = (queue.items || []).filter(
    (i) => i.account === CONFIG.account && i.platform === CONFIG.platform
  );
  summary.queue_items = items.length;

  const now = Date.now();
  const dueItems = items
    .filter((i) => Date.parse(i.publish_at) <= now)
    .sort((a, b) => Date.parse(a.publish_at) - Date.parse(b.publish_at));
  const next = items
    .filter((i) => Date.parse(i.publish_at) > now)
    .sort((a, b) => Date.parse(a.publish_at) - Date.parse(b.publish_at))[0];
  summary.next_item = next
    ? { item_id: next.item_id, publish_at: next.publish_at }
    : null;

  // Recent posts, used only as a duplicate guard for items whose previous
  // attempt ended in an unknown state (posting/failed).
  let recent = [];
  const ambiguous = [];
  for (const item of dueItems) {
    const prevRaw = await env.STATE.get(`result:${item.item_id}`);
    const prev = prevRaw ? JSON.parse(prevRaw) : null;
    item._prev = prev;
    if (prev && (prev.status === "posting" || prev.status === "failed")) {
      ambiguous.push(item);
    }
  }
  if (ambiguous.length) {
    try {
      const r = await threadsFetch(
        "/me/threads?fields=id,text,permalink&limit=25",
        token
      );
      recent = r.data || [];
    } catch (_) {
      // guard unavailable; fall through to normal posting
    }
  }

  for (const item of dueItems) {
    const key = `result:${item.item_id}`;
    const prev = item._prev;
    if (prev && prev.status === "posted") {
      summary.skipped.push({ item_id: item.item_id, why: "already posted" });
      continue;
    }
    if (prev && prev.status === "failed_final") {
      summary.skipped.push({ item_id: item.item_id, why: "failed_final" });
      continue;
    }
    if (prev && prev.status === "rate_limited") {
      summary.skipped.push({ item_id: item.item_id, why: "rate_limited" });
      continue;
    }
    // Duplicate guard: a previous attempt may have published without its
    // result being recorded. Match by exact text before re-posting.
    if (prev && (prev.status === "posting" || prev.status === "failed")) {
      const dup = recent.find((p) => p.text === item.text);
      if (dup) {
        if (!dry) {
          await env.STATE.put(
            key,
            JSON.stringify({
              status: "posted",
              attempts: prev.attempts || 1,
              post_id: dup.id,
              permalink: dup.permalink || null,
              recovered: true,
              updated_at: new Date().toISOString(),
              source_cron_id: item.source_cron_id || null,
            })
          );
        }
        summary.posted.push({
          item_id: item.item_id,
          recovered: true,
          permalink: dup.permalink || null,
        });
        continue;
      }
    }
    if (prev && (prev.attempts || 0) >= CONFIG.maxAttempts) {
      if (!dry) {
        await env.STATE.put(
          key,
          JSON.stringify({
            ...prev,
            status: "failed_final",
            updated_at: new Date().toISOString(),
          })
        );
      }
      summary.skipped.push({ item_id: item.item_id, why: "attempts exhausted" });
      continue;
    }

    summary.due.push({
      item_id: item.item_id,
      publish_at: item.publish_at,
      prev_status: prev ? prev.status : null,
      attempts: prev ? prev.attempts || 0 : 0,
    });
    if (dry) continue;

    const attempts = (prev ? prev.attempts || 0 : 0) + 1;
    await env.STATE.put(
      key,
      JSON.stringify({
        status: "posting",
        attempts,
        updated_at: new Date().toISOString(),
        source_cron_id: item.source_cron_id || null,
      })
    );
    try {
      const { postId, permalink } = await postToThreads(me, token, item.text);
      await env.STATE.put(
        key,
        JSON.stringify({
          status: "posted",
          attempts,
          post_id: postId,
          permalink,
          posted_at: new Date().toISOString(),
          source_cron_id: item.source_cron_id || null,
        })
      );
      summary.posted.push({ item_id: item.item_id, permalink });
    } catch (e) {
      if (e instanceof RateLimitError) {
        await env.STATE.put(
          key,
          JSON.stringify({
            status: "rate_limited",
            attempts: prev ? prev.attempts || 0 : 0,
            updated_at: new Date().toISOString(),
            source_cron_id: item.source_cron_id || null,
            note: "429: aborted this run per standing rule",
          })
        );
        summary.errors.push({ item_id: item.item_id, error: "rate_limited" });
        break; // 429 stops the whole run
      }
      const status =
        attempts >= CONFIG.maxAttempts ? "failed_final" : "failed";
      await env.STATE.put(
        key,
        JSON.stringify({
          status,
          attempts,
          error: String((e && e.message) || e).slice(0, 300),
          updated_at: new Date().toISOString(),
          source_cron_id: item.source_cron_id || null,
        })
      );
      summary.errors.push({
        item_id: item.item_id,
        error: String((e && e.message) || e).slice(0, 200),
      });
    }
  }

  if (!dry) {
    await env.STATE.put("state:last_run", JSON.stringify(summary));
  }
  return summary;
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      run(env).catch(async (e) => {
        try {
          await env.STATE.put(
            "state:last_run",
            JSON.stringify({
              at: new Date().toISOString(),
              fatal: String((e && e.message) || e).slice(0, 300),
            })
          );
        } catch (_) {}
      })
    );
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__dryrun") {
      try {
        return Response.json(await run(env, { dry: true }));
      } catch (e) {
        return Response.json(
          { error: String((e && e.message) || e).slice(0, 300) },
          { status: 500 }
        );
      }
    }
    if (url.pathname === "/__status") {
      const last = await env.STATE.get("state:last_run");
      return Response.json({
        ok: true,
        last_run: last ? JSON.parse(last) : null,
      });
    }
    return Response.json({ ok: true, service: "social-auto-poster" });
  },
};
