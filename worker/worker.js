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
// Instagram lane (added 2026-10-27 at the user's request):
//   ruri_instagram -> KV token:instagram:ruri + id:instagram:ruri
// Queue items with platform "instagram" carry a public video_url and use
// item.text as the caption. Reels flow: create media container -> poll
// status_code until FINISHED (a still-processing container is resumed on
// the next cron run via the recorded container_id) -> media_publish.
// Media files themselves live in KV under media:<name> and are served
// publicly at /media/<name> (Meta's fetcher needs a durable public URL).
//
// KV binding: STATE (namespace "auto-poster")
//   token:threads:<account> -> long-lived Threads token (user-saved)
//   token:instagram:ruri     -> long-lived Instagram token (user-saved)
//   id:instagram:ruri        -> Instagram user id for the ruri account
//   media:<name>             -> media bytes served at /media/<name>
//   result:<item_id>         -> per-item result JSON (double-post guard)
//   state:last_run           -> summary of the most recent scheduled run
//
// Tree posts: an item with reply_to_item_id is posted as a reply to the
// post recorded under result:<reply_to_item_id>. If the parent is not
// posted yet, the item waits for a later run (no attempt is consumed).
//
// External comment replies: an item with reply_to_post_id (a Threads
// post/comment id NOT produced by this queue — e.g. another person's
// comment on one of our posts) is posted as a reply to that id directly.
// reply_to_post_id takes precedence when both fields are set. Approval
// for such items happens before queueing (see REPLY-FLOW.md); once
// queued, the Worker treats them like any other due item, including the
// record-"posting"-first duplicate safety.
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
  igGraph: "https://graph.instagram.com/v21.0",
  maxAttempts: 3,
  igPollRounds: 12,
  igPollIntervalMs: 5000,
};

const ACCOUNTS = [
  {
    account: "ruri",
    tokenKey: "token:threads:ruri",
    platform: "threads",
    lane: "ruri",
  },
  {
    account: "soseki",
    tokenKey: "token:threads:soseki",
    platform: "threads",
    lane: "soseki",
  },
  {
    account: "threads_hoshimi",
    tokenKey: "token:threads:threads_hoshimi",
    platform: "threads",
    lane: "threads_hoshimi",
  },
  {
    account: "ruri",
    tokenKey: "token:instagram:ruri",
    idKey: "id:instagram:ruri",
    platform: "instagram",
    lane: "ruri_instagram",
  },
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

async function igFetch(path, token, options = {}) {
  const resp = await fetch(CONFIG.igGraph + path, {
    ...options,
    headers: { Authorization: `Bearer ${token}`, ...(options.headers || {}) },
  });
  if (resp.status === 429) throw new RateLimitError("instagram http 429");
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg =
      (data && data.error && (data.error.message || data.error.type)) ||
      `http ${resp.status}`;
    if (data && data.error && data.error.code === 4) {
      throw new RateLimitError("instagram error code 4 (rate limit)");
    }
    throw new Error(`instagram api: ${msg}`);
  }
  return data;
}

async function igPostForm(path, token, params) {
  const body = new URLSearchParams(params);
  return igFetch(path, token, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
}

// Polls a media container until FINISHED. Returns "FINISHED" or
// "IN_PROGRESS" (budget exhausted; caller records the container and a
// later run resumes it). ERROR / EXPIRED throw.
async function pollIgContainer(containerId, token) {
  for (let i = 0; i < CONFIG.igPollRounds; i++) {
    if (i > 0) await sleep(CONFIG.igPollIntervalMs);
    const s = await igFetch(
      `/${containerId}?fields=status_code,status`,
      token
    );
    const code = s.status_code || "IN_PROGRESS";
    if (code === "FINISHED") return "FINISHED";
    if (code === "ERROR" || code === "EXPIRED") {
      throw new Error(
        `container ${code.toLowerCase()}: ${s.status || "no detail"}`
      );
    }
  }
  return "IN_PROGRESS";
}

// Publishes an already-FINISHED container and resolves its permalink.
async function publishIgContainer(igId, token, containerId) {
  const published = await igPostForm(`/${igId}/media_publish`, token, {
    creation_id: containerId,
  });
  let permalink = null;
  try {
    const p = await igFetch(`/${published.id}?fields=permalink`, token);
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
    // External reply target (another person's post/comment id), if any.
    const extReplyId =
      typeof item.reply_to_post_id === "string" && item.reply_to_post_id.trim()
        ? item.reply_to_post_id.trim()
        : null;
    const extMeta = extReplyId ? { reply_to_post_id: extReplyId } : {};
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
              ...extMeta,
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
    let replyToId = extReplyId;
    let parentPendingDry = false;
    if (item.reply_to_item_id && !extReplyId) {
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
      reply_to_post_id: extReplyId,
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
        ...extMeta,
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
          ...extMeta,
          ...(cond ? { condition: cond, metrics: observed } : {}),
        })
      );
      summary.posted.push({
        item_id: item.item_id,
        permalink,
        ...extMeta,
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
            ...extMeta,
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
          ...extMeta,
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

// Instagram lane: Reels only. Items carry video_url (public) and use
// item.text as the caption. Same safety rules as the Threads lanes:
// record-before-posting duplicate guard, caption-match recovery for
// ambiguous attempts, max attempts, 429 stops the lane for this run.
// A container still processing when the poll budget ends is recorded
// under status "processing" with its container_id; the next run resumes
// it (publish once FINISHED) without consuming another attempt.
async function runInstagramAccount(env, cfg, queue, { dry = false } = {}) {
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

  const me = await igFetch("/me?fields=user_id,username", token);
  summary.username = me.username || null;
  const igId = (cfg.idKey && (await env.STATE.get(cfg.idKey))) || me.user_id;
  if (!igId) {
    summary.errors.push("instagram user id unavailable (kv + /me)");
    return summary;
  }

  const items = (queue.items || []).filter(
    (i) => i.account === cfg.account && i.platform === "instagram"
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

  // Recent media, used only as a duplicate guard for items whose
  // previous attempt ended in an unknown state (posting/failed).
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
      const r = await igFetch(
        "/me/media?fields=id,caption,permalink&limit=25",
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
    if (prev && (prev.status === "posting" || prev.status === "failed")) {
      const dup = recent.find((p) => p.caption === item.text);
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

    summary.due.push({
      item_id: item.item_id,
      publish_at: item.publish_at,
      prev_status: prev ? prev.status : null,
      attempts: prev ? prev.attempts || 0 : 0,
      video_url: item.video_url || null,
      ...(prev && prev.status === "processing" && prev.container_id
        ? { resume_container_id: prev.container_id }
        : {}),
    });
    if (dry) continue;

    try {
      // Resume path: a container from an earlier run may now be ready.
      if (prev && prev.status === "processing" && prev.container_id) {
        const state = await pollIgContainer(prev.container_id, token);
        if (state === "IN_PROGRESS") {
          summary.skipped.push({
            item_id: item.item_id,
            why: "container still processing",
          });
          continue;
        }
        const { postId, permalink } = await publishIgContainer(
          igId,
          token,
          prev.container_id
        );
        await env.STATE.put(
          key,
          JSON.stringify({
            status: "posted",
            attempts: prev.attempts || 1,
            post_id: postId,
            permalink,
            posted_at: new Date().toISOString(),
            source_cron_id: item.source_cron_id || null,
          })
        );
        summary.posted.push({ item_id: item.item_id, permalink });
        continue;
      }

      if (!item.video_url) {
        await env.STATE.put(
          key,
          JSON.stringify({
            status: "failed_final",
            attempts: prev ? prev.attempts || 0 : 0,
            error: "queue item has no video_url",
            updated_at: new Date().toISOString(),
            source_cron_id: item.source_cron_id || null,
          })
        );
        summary.errors.push({
          item_id: item.item_id,
          error: "missing video_url",
        });
        continue;
      }
      if (prev && (prev.attempts || 0) >= CONFIG.maxAttempts) {
        await env.STATE.put(
          key,
          JSON.stringify({
            ...prev,
            status: "failed_final",
            updated_at: new Date().toISOString(),
          })
        );
        summary.skipped.push({
          item_id: item.item_id,
          why: "attempts exhausted",
        });
        continue;
      }

      const attempts = (prev ? prev.attempts || 0 : 0) + 1;
      await env.STATE.put(
        key,
        JSON.stringify({
          status: "processing",
          stage: "creating",
          attempts,
          updated_at: new Date().toISOString(),
          source_cron_id: item.source_cron_id || null,
        })
      );
      const container = await igPostForm(`/${igId}/media`, token, {
        media_type: "REELS",
        video_url: item.video_url,
        caption: item.text || "",
      });
      await env.STATE.put(
        key,
        JSON.stringify({
          status: "processing",
          stage: "processing",
          attempts,
          container_id: container.id,
          updated_at: new Date().toISOString(),
          source_cron_id: item.source_cron_id || null,
        })
      );
      const state = await pollIgContainer(container.id, token);
      if (state === "IN_PROGRESS") {
        summary.skipped.push({
          item_id: item.item_id,
          why: "container still processing",
        });
        continue;
      }
      const { postId, permalink } = await publishIgContainer(
        igId,
        token,
        container.id
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
        break; // 429 stops this lane's run
      }
      const attempts =
        prev && prev.status === "processing"
          ? prev.attempts || 1
          : (prev ? prev.attempts || 0 : 0) + 1;
      const status = attempts >= CONFIG.maxAttempts ? "failed_final" : "failed";
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
    const laneKey = cfg.lane || cfg.account;
    try {
      summary.accounts[laneKey] =
        cfg.platform === "instagram"
          ? await runInstagramAccount(env, cfg, queue, { dry })
          : await runAccount(env, cfg, queue, { dry });
    } catch (e) {
      summary.accounts[laneKey] = {
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
    // Public media hosting for Instagram video_url: bytes live in KV
    // under media:<name>. Meta's fetcher requires a durable public URL.
    if (url.pathname.startsWith("/media/") && request.method === "GET") {
      const name = decodeURIComponent(url.pathname.slice("/media/".length));
      if (!name || name.includes("/") || name.includes("..")) {
        return Response.json({ error: "bad media name" }, { status: 400 });
      }
      const buf = await env.STATE.get(`media:${name}`, "arrayBuffer");
      if (!buf) {
        return Response.json({ error: "media not found" }, { status: 404 });
      }
      const types = {
        ".mp4": "video/mp4",
        ".mov": "video/quicktime",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
      };
      const ext = name.slice(name.lastIndexOf(".")).toLowerCase();
      return new Response(buf, {
        headers: {
          "Content-Type": types[ext] || "application/octet-stream",
          "Cache-Control": "public, max-age=3600",
        },
      });
    }
    return Response.json({ ok: true, service: "social-auto-poster" });
  },
};
