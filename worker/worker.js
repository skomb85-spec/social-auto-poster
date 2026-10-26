// social-auto-poster — Phase 1+ (ruri + soseki Threads)
//
// Runs on a Cloudflare Cron Trigger (every 30 min). Reads the approved
// queue from this repo's queue.json (raw URL), posts due items to the
// Threads API with each account's long-lived token stored in Workers KV,
// and records per-item results back into KV. No secrets live in this file.
//
// Accounts (added soseki on 2026-10-25 at the user's request; soseki
// Instagram stays out of scope):
//   ruri   -> KV token:threads:ruri
//   soseki -> KV token:threads:soseki
//
// KV binding: STATE (namespace "auto-poster")
//   token:threads:<account> -> long-lived Threads token (user-saved)
//   result:<item_id>         -> per-item result JSON (double-post guard)
//   state:last_run           -> summary of the most recent scheduled run
//
// Tree posts: an item with reply_to_item_id is posted as a reply to the
// post recorded under result:<reply_to_item_id>. If the parent is not
// posted yet, the item waits for a later run (no attempt is consumed).
//
// Conditional CTA replies: an item may carry
//   condition: { min_views?: int, min_likes?: int }
// Such an item (normally a reply scheduled ~1h after its parent) posts
// only if the parent post's observed Threads metrics meet at least one
// specified threshold (OR). Unmet -> result "skipped_condition" (final,
// no retry). Metrics unreadable or unverifiable -> result
// "skipped_condition_error" (final): a gated CTA is never posted blind.
//
// HTTP endpoints (read-only, for verification):
//   /           -> service banner
//   /__status   -> last scheduled run summary from KV
//   /__dryrun   -> per-account token whoami + queue read + due judgement,
//                  no posting

const CONFIG = {
  queueUrl:
    "https://raw.githubusercontent.com/skomb85-spec/social-auto-poster/main/queue.json",
  graph: "https://graph.threads.net/v1.0",
  maxAttempts: 3,
};

const ACCOUNTS = [
  { account: "ruri", tokenKey: "token:threads:ruri", platform: "threads" },
  { account: "soseki", tokenKey: "token:threads:soseki", platform: "threads" },
];

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

async function postToThreads(me, token, text, replyToId) {
  const params = { media_type: "TEXT", text };
  if (replyToId) params.reply_to_id = replyToId;
  const container = await threadsPostForm(`/${me.id}/threads`, token, params);
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

// Returns the effective gate {min_views?, min_likes?} or null when the
// item carries no usable condition (legacy behaviour: always post).
function effectiveCondition(condition) {
  if (!condition || typeof condition !== "object") return null;
  const out = {};
  if (Number.isFinite(condition.min_views)) out.min_views = condition.min_views;
  if (Number.isFinite(condition.min_likes)) out.min_likes = condition.min_likes;
  return Object.keys(out).length ? out : null;
}

// Reads a post's observed metrics via the Insights endpoint. A metric
// absent from the response stays null (unverifiable), never zero.
async function getPostMetrics(token, postId) {
  const data = await threadsFetch(
    `/${postId}/insights?metric=views,likes`,
    token
  );
  const out = { views: null, likes: null };
  for (const m of (data && data.data) || []) {
    const raw =
      (m.values && m.values[0] && m.values[0].value) ??
      (m.total_value && m.total_value.value);
    if (typeof raw !== "number") continue;
    if (m.name === "views") out.views = raw;
    if (m.name === "likes") out.likes = raw;
  }
  return out;
}

async function runAccount(env, cfg, queue, { dry = false } = {}) {
  const summary = {
    username: null,
    queue_items: 0,
    due: [],
    posted: [],
    skipped: [],
    errors: [],
    next_item: null,
  };

  const token = await env.STATE.get(cfg.tokenKey);
  if (!token) {
    summary.errors.push(`kv token missing: ${cfg.tokenKey}`);
    return summary;
  }

  // Whoami: validates the token on every run and resolves the user id.
  const me = await threadsFetch("/me?fields=id,username", token);
  summary.username = me.username || null;

  const items = (queue.items || []).filter(
    (i) => i.account === cfg.account && i.platform === cfg.platform
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
    if (prev && prev.status === "skipped_condition") {
      summary.skipped.push({ item_id: item.item_id, why: "condition not met" });
      continue;
    }
    if (prev && prev.status === "skipped_condition_error") {
      summary.skipped.push({
        item_id: item.item_id,
        why: "condition check failed (permanent)",
      });
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

    // Tree posts: resolve the parent post id from KV results. If the
    // parent is not posted yet, wait for a later run without consuming
    // an attempt.
    let replyToId = null;
    let parentPendingDry = false;
    if (item.reply_to_item_id) {
      const parentRaw = await env.STATE.get(`result:${item.reply_to_item_id}`);
      const parent = parentRaw ? JSON.parse(parentRaw) : null;
      if (parent && parent.status === "posted" && parent.post_id) {
        replyToId = parent.post_id;
      } else if (dry) {
        // In dry runs the parent may be due in the same run; report the
        // chain as resolvable when the parent is itself a queue item.
        const parentItem = (queue.items || []).find(
          (i) => i.item_id === item.reply_to_item_id
        );
        if (!parentItem) {
          summary.skipped.push({
            item_id: item.item_id,
            why: "parent not posted yet",
          });
          continue;
        }
        parentPendingDry = true;
      } else {
        summary.skipped.push({
          item_id: item.item_id,
          why: "parent not posted yet",
        });
        continue;
      }
    }

    // Conditional CTA gate: when the item carries a condition, measure
    // the parent post before posting. A skip here is final and consumes
    // no posting attempt. In dry runs this is read-only diagnostics:
    // unmet items are reported as skipped, nothing is written.
    const cond = effectiveCondition(item.condition);
    let observed = null;
    if (cond && !parentPendingDry) {
      if (!replyToId) {
        if (!dry) {
          await env.STATE.put(
            key,
            JSON.stringify({
              status: "skipped_condition_error",
              attempts: prev ? prev.attempts || 0 : 0,
              condition: cond,
              reason:
                "condition set but no parent post to measure (reply_to_item_id missing or parent has no post_id)",
              updated_at: new Date().toISOString(),
              source_cron_id: item.source_cron_id || null,
            })
          );
        }
        summary.skipped.push({
          item_id: item.item_id,
          why: "condition: no parent post",
        });
        continue;
      }
      try {
        observed = await getPostMetrics(token, replyToId);
      } catch (e) {
        if (e instanceof RateLimitError) {
          if (dry) {
            summary.errors.push({
              item_id: item.item_id,
              error: "rate_limited (metrics fetch)",
            });
            continue;
          }
          await env.STATE.put(
            key,
            JSON.stringify({
              status: "rate_limited",
              attempts: prev ? prev.attempts || 0 : 0,
              updated_at: new Date().toISOString(),
              source_cron_id: item.source_cron_id || null,
              note: "429 during condition metrics fetch: aborted this run per standing rule",
            })
          );
          summary.errors.push({ item_id: item.item_id, error: "rate_limited" });
          break; // 429 stops this account's run
        }
        if (!dry) {
          await env.STATE.put(
            key,
            JSON.stringify({
              status: "skipped_condition_error",
              attempts: prev ? prev.attempts || 0 : 0,
              condition: cond,
              reason: "metrics fetch failed",
              error: String((e && e.message) || e).slice(0, 300),
              updated_at: new Date().toISOString(),
              source_cron_id: item.source_cron_id || null,
            })
          );
        }
        summary.skipped.push({
          item_id: item.item_id,
          why: "condition: metrics fetch failed",
        });
        continue;
      }
      const met =
        (cond.min_views != null &&
          observed.views != null &&
          observed.views >= cond.min_views) ||
        (cond.min_likes != null &&
          observed.likes != null &&
          observed.likes >= cond.min_likes);
      if (!met) {
        // A specified metric that came back null could not be verified;
        // that is a check failure (permanent), not a clean "not met".
        const unverifiable =
          (cond.min_views != null && observed.views == null) ||
          (cond.min_likes != null && observed.likes == null);
        if (!dry) {
          await env.STATE.put(
            key,
            JSON.stringify({
              status: unverifiable
                ? "skipped_condition_error"
                : "skipped_condition",
              attempts: prev ? prev.attempts || 0 : 0,
              condition: cond,
              metrics: observed,
              ...(unverifiable
                ? { reason: "specified metric unavailable in insights response" }
                : {}),
              updated_at: new Date().toISOString(),
              source_cron_id: item.source_cron_id || null,
            })
          );
        }
        summary.skipped.push({
          item_id: item.item_id,
          why: unverifiable
            ? "condition: metric unavailable"
            : "condition not met",
          metrics: observed,
        });
        continue;
      }
    }

    summary.due.push({
      item_id: item.item_id,
      publish_at: item.publish_at,
      prev_status: prev ? prev.status : null,
      attempts: prev ? prev.attempts || 0 : 0,
      reply_to_item_id: item.reply_to_item_id || null,
      ...(cond ? { condition: cond, metrics: observed } : {}),
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
      const { postId, permalink } = await postToThreads(
        me,
        token,
        item.text,
        replyToId
      );
      await env.STATE.put(
        key,
        JSON.stringify({
          status: "posted",
          attempts,
          post_id: postId,
          permalink,
          posted_at: new Date().toISOString(),
          source_cron_id: item.source_cron_id || null,
          ...(cond ? { condition: cond, metrics: observed } : {}),
        })
      );
      summary.posted.push({
        item_id: item.item_id,
        permalink,
        ...(cond ? { metrics: observed } : {}),
      });
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
        break; // 429 stops this account's run
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
          ...(cond ? { condition: cond, metrics: observed } : {}),
        })
      );
      summary.errors.push({
        item_id: item.item_id,
        error: String((e && e.message) || e).slice(0, 200),
      });
    }
  }

  return summary;
}

async function run(env, { dry = false } = {}) {
  const startedAt = new Date().toISOString();
  const summary = { at: startedAt, dry, accounts: {}, errors: [] };

  const qResp = await fetch(`${CONFIG.queueUrl}?t=${Date.now()}`, {
    headers: { "User-Agent": "social-auto-poster" },
    cf: { cacheTtl: 0, cacheEverything: false },
  });
  if (!qResp.ok) {
    summary.errors.push(`queue fetch http ${qResp.status}`);
    return summary;
  }
  const queue = await qResp.json();

  for (const cfg of ACCOUNTS) {
    try {
      summary.accounts[cfg.account] = await runAccount(env, cfg, queue, {
        dry,
      });
    } catch (e) {
      summary.accounts[cfg.account] = {
        username: null,
        queue_items: 0,
        due: [],
        posted: [],
        skipped: [],
        errors: [String((e && e.message) || e).slice(0, 300)],
        next_item: null,
      };
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
