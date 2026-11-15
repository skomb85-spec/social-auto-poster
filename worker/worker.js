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
// soseki tree delay (2026-11-14, user request): for account "soseki",
// tree replies are additionally held until 1 hour after the ROOT post
// of the chain was posted, so the whole tree lands as one block about
// an hour after the parent instead of immediately. Other accounts are
// unaffected.
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
// Comment auto-reply lane (added 2026-11-15 at the user's request,
// replacing the agent-run comment-check crons): on every run, for the
// soseki and ruri Threads accounts only (hoshimi stays discovery-only
// and is never replied to), the Worker scans our own posts from the
// last 3 hours (ruri solicitation posts: 24 hours) for comments by
// other people and replies from the pre-approved pattern sets
// (soseki reply-knowledge types 1-5; ruri interaction types A-E).
// Rotation: never the same type or the same opening as the account's
// previous reply. Hold categories (recorded, never replied):
// criticism/anti, health/medical/mental consultations, deep personal
// circumstances, promo/sales — keyword heuristics, biased to hold.
// Max 3 replies per account per run. A permission error stops that
// account's reply attempts and is recorded as 要対応：権限エラー until
// the KV token is replaced (detected via a token fingerprint).
// State lives in KV:
//   comment-state:<account>  -> seen comment ids, rotation, perm flag
//   reply:<account>:<comment_id> -> per-reply record (the results log)
// Per-run outcomes are summarised under summary.comments, which lands
// in state:last_run like every other lane.
//
// HTTP endpoints (read-only, for verification):
//   /           -> service banner
//   /__status   -> last scheduled run summary from KV
//   /__dryrun   -> per-account token whoami + queue read + due judgement,
//                  no posting
//
// Notion queue source (added 2026-11-06): when the NOTION_TOKEN secret
// is set, rows with status "queued" in the Notion database
// CONFIG.notionDbId are fetched on every run and merged into the queue
// (GitHub items win on item_id collisions). Outcomes of Notion-sourced
// items are written back to their rows (status -> posted/failed, plus
// result_url when a permalink is known). Without the secret the Notion
// source is disabled and behaviour is exactly the GitHub-only flow.

const CONFIG = {
  queueUrl:
    "https://raw.githubusercontent.com/skomb85-spec/social-auto-poster/main/queue.json",
  notionDbId: "eb149a9566fb4debb473cfd5f0170372",
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
    account: "threads_tomurai",
    tokenKey: "token:threads:threads_tomurai",
    platform: "threads",
    lane: "threads_tomurai",
  },
  {
    account: "threads_makanai",
    tokenKey: "token:threads:threads_makanai",
    platform: "threads",
    lane: "threads_makanai",
  },
  {
    account: "threads_tenki",
    tokenKey: "token:threads:threads_tenki",
    platform: "threads",
    lane: "threads_tenki",
  },
  {
    account: "threads_self",
    tokenKey: "token:threads:threads_tenki",
    platform: "threads",
    lane: "threads_self",
  },
  {
    account: "threads_batsuko",
    tokenKey: "token:threads:threads_hoshimi",
    platform: "threads",
    lane: "threads_batsuko",
  },
  {
    account: "mu",
    tokenKey: "token:threads:mu",
    platform: "threads",
    lane: "mu",
  },
  {
    account: "kohaku",
    tokenKey: "token:threads:kohaku",
    platform: "threads",
    lane: "kohaku",
  },
  {
    account: "ruri",
    tokenKey: "token:instagram:ruri",
    idKey: "id:instagram:ruri",
    platform: "instagram",
    lane: "ruri_instagram",
  },
  {
    account: "japancostlab",
    tokenKey: "token:threads:japancostlab",
    platform: "threads",
    lane: "japancostlab",
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

// ---- Notion queue source -------------------------------------------------

function notionRichText(prop) {
  if (!prop || !Array.isArray(prop.rich_text)) return "";
  return prop.rich_text.map((t) => t.plain_text || "").join("");
}

// Converts one Notion database page into a queue item. Rows that lack
// item_id, account or publish_at cannot be scheduled safely, so they
// are skipped and reported through `errors` instead. The Instagram
// lane (lane name "ruri_instagram") selects queue items by account
// "ruri" + platform "instagram", so a row addressed by that lane name
// is normalized to the values the lane filter matches. The row's
// video_url column maps to item.video_url, which the Reels flow
// requires (item.text is the caption).
function notionPageToItem(page, errors) {
  const p = page.properties || {};
  const itemId = ((p.item_id && p.item_id.title) || [])
    .map((t) => t.plain_text || "")
    .join("")
    .trim();
  const rawAccount =
    p.account && p.account.select ? p.account.select.name : null;
  const rawPlatform =
    p.platform && p.platform.select ? p.platform.select.name : null;
  const publishAt =
    p.publish_at && p.publish_at.date ? p.publish_at.date.start : null;
  if (!itemId || !rawAccount || !publishAt) {
    const missing = [
      !itemId ? "item_id" : null,
      !rawAccount ? "account" : null,
      !publishAt ? "publish_at" : null,
    ]
      .filter(Boolean)
      .join(", ");
    errors.push(`notion row ${page.id}: skipped, missing ${missing}`);
    return null;
  }
  let account = rawAccount;
  let platform = rawPlatform;
  if (rawAccount === "ruri_instagram") {
    account = "ruri";
    platform = "instagram";
  }
  const mediaRaw = notionRichText(p.media);
  const media = mediaRaw
    ? mediaRaw
        .split(/[\n,]/)
        .map((s) => s.trim())
        .filter(Boolean)
    : [];
  const replyToPostId = notionRichText(p.reply_to_post_id).trim();
  const replyToItemId = notionRichText(p.reply_to_item_id).trim();
  const videoUrl = notionRichText(p.video_url).trim();
  return {
    item_id: itemId,
    source_cron_id: null,
    account,
    platform,
    publish_at: publishAt,
    text: notionRichText(p.text),
    media,
    video_url: videoUrl || null,
    reply_to_item_id: replyToItemId || null,
    reply_to_post_id: replyToPostId || null,
    source: "notion",
    notion_page_id: page.id,
  };
}

// The Notion token comes from the NOTION_TOKEN secret when present, and
// otherwise from the KV key "token:notion" (dashboard-saved). The KV path
// keeps the Notion source alive across API-driven version deploys, which
// cannot carry secret_text values.
async function resolveNotionToken(env) {
  if (env.NOTION_TOKEN) return env.NOTION_TOKEN;
  try {
    return (await env.STATE.get("token:notion")) || null;
  } catch {
    return null;
  }
}

// Fetches every "queued" row of the Notion queue database. Returns
// { enabled, items, errors }; row-level problems land in errors, while
// transport/API failures throw so the caller can isolate them from the
// GitHub queue flow. Without a Notion token the source is disabled
// and this is a no-op.
async function fetchNotionQueue(env) {
  const token = await resolveNotionToken(env);
  const out = { enabled: !!token, items: [], errors: [] };
  if (!token) return out;
  let cursor;
  do {
    const resp = await fetch(
      `https://api.notion.com/v1/databases/${CONFIG.notionDbId}/query`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          filter: { property: "status", select: { equals: "queued" } },
          page_size: 100,
          ...(cursor ? { start_cursor: cursor } : {}),
        }),
      }
    );
    if (!resp.ok) {
      const detail = await resp.text().catch(() => "");
      throw new Error(
        `notion query http ${resp.status}: ${detail.slice(0, 200)}`
      );
    }
    const data = await resp.json();
    for (const page of data.results || []) {
      const item = notionPageToItem(page, out.errors);
      if (item) out.items.push(item);
    }
    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);
  return out;
}

// Mirrors a finished Notion-sourced item's outcome back to its row:
// status select -> "posted" / "failed", plus result_url when the post
// permalink is a URL. Best-effort by design: failures are logged into
// the lane summary and never fail the run (the KV result remains the
// double-post guard of record).
async function notionWriteBack(env, summary, item, status, permalink) {
  if (!item || item.source !== "notion" || !item.notion_page_id) return;
  const token = await resolveNotionToken(env);
  if (!token) return;
  const properties = { status: { select: { name: status } } };
  if (
    status === "posted" &&
    typeof permalink === "string" &&
    permalink.startsWith("http")
  ) {
    properties.result_url = { url: permalink };
  }
  try {
    const resp = await fetch(
      `https://api.notion.com/v1/pages/${item.notion_page_id}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ properties }),
      }
    );
    if (!resp.ok) {
      const detail = await resp.text().catch(() => "");
      throw new Error(
        `notion patch http ${resp.status}: ${detail.slice(0, 200)}`
      );
    }
  } catch (e) {
    summary.errors.push({
      item_id: item.item_id,
      error: `notion write-back failed: ${String(
        (e && e.message) || e
      ).slice(0, 200)}`,
    });
  }
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
          await notionWriteBack(
            env,
            summary,
            item,
            "posted",
            dup.permalink || null
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
        await notionWriteBack(env, summary, item, "failed", null);
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

    // One-hour live delay: soseki trees and ruri affiliate CTA replies.
    // Ruri applies only to explicitly named affiliate CTA items; other
    // ruri replies remain unaffected. The clock starts at the ROOT post's
    // actual KV posted_at, not merely its scheduled publish_at.
    // Waiting consumes no attempt; dry runs bypass this gate.
    const ruriAffiliateCta =
      item.account === "ruri" &&
      typeof item.item_id === "string" &&
      /^ruri-aff-\d{8}-[a-z0-9-]+-cta$/.test(item.item_id);
    if (
      !dry &&
      (item.account === "soseki" || ruriAffiliateCta) &&
      item.reply_to_item_id &&
      !extReplyId
    ) {
      let rootId = null;
      const seenIds = new Set([item.item_id]);
      let cursor = item;
      while (cursor && cursor.reply_to_item_id) {
        const pid = cursor.reply_to_item_id;
        if (seenIds.has(pid)) break;
        seenIds.add(pid);
        const parentItem = (queue.items || []).find(
          (i) => i.item_id === pid
        );
        if (!parentItem || !parentItem.reply_to_item_id) {
          rootId = pid;
          break;
        }
        cursor = parentItem;
      }
      if (rootId) {
        const rootRaw = await env.STATE.get(`result:${rootId}`);
        const root = rootRaw ? JSON.parse(rootRaw) : null;
        const rootPostedAt =
          root && root.status === "posted"
            ? Date.parse(root.posted_at || root.updated_at || "")
            : NaN;
        if (ruriAffiliateCta && !Number.isFinite(rootPostedAt)) {
          summary.skipped.push({
            item_id: item.item_id,
            why: "affiliate CTA waiting: parent posted_at unavailable",
          });
          continue;
        }
        if (
          Number.isFinite(rootPostedAt) &&
          Date.now() < rootPostedAt + 60 * 60 * 1000
        ) {
          summary.skipped.push({
            item_id: item.item_id,
            why: ruriAffiliateCta
              ? "affiliate CTA delay: parent posted less than 1h ago"
              : "tree delay: root posted less than 1h ago",
          });
          continue;
        }
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
      await notionWriteBack(env, summary, item, "posted", permalink);
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
      if (status === "failed_final") {
        await notionWriteBack(env, summary, item, "failed", null);
      }
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
          await notionWriteBack(
            env,
            summary,
            item,
            "posted",
            dup.permalink || null
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
        await notionWriteBack(env, summary, item, "posted", permalink);
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
        await notionWriteBack(env, summary, item, "failed", null);
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
        await notionWriteBack(env, summary, item, "failed", null);
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
      await notionWriteBack(env, summary, item, "posted", permalink);
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
      if (status === "failed_final") {
        await notionWriteBack(env, summary, item, "failed", null);
      }
      summary.errors.push({
        item_id: item.item_id,
        error: String((e && e.message) || e).slice(0, 200),
      });
    }
  }

  return summary;
}

// ---- Comment auto-reply lane (soseki + ruri, added 2026-11-15) ----------
//
// Design notes:
// - Discovery mirrors discover_comments.py: /me/threads -> per post
//   /{post_id}/replies, own-username replies filtered out.
// - Replies are posted with the same container flow as queue items
//   (postToThreads with reply_to_id = the comment id).
// - Reply texts are template instantiations of the pre-approved
//   pattern sets. Soseki: reply-knowledge.md types 1-5 (quiet voice,
//   no emoji, no links). Ruri: research-knowledge section 6b types
//   A-E (empathy first, know-how type E at most 1 in the last 5).
// - When in doubt a comment is held, not answered.

const COMMENT_LANE = {
  accounts: [
    { account: "soseki", tokenKey: "token:threads:soseki" },
    { account: "ruri", tokenKey: "token:threads:ruri" },
  ],
  windowMs: 3 * 60 * 60 * 1000,
  ruriSolicitWindowMs: 24 * 60 * 60 * 1000,
  maxRepliesPerRun: 3,
  maxCandidatesPerRun: 20,
  postsLimit: 25,
  stateKeepMs: 4 * 24 * 60 * 60 * 1000,
};

// Hold = record only, never reply. Keyword heuristics; the bias is to
// hold whenever a category might apply.
const HOLD_PATTERNS = [
  { reason: "promo", re: /https?:\/\/|www\./i },
  {
    reason: "promo",
    re: /稼げ|副業で|月収\s*\d|アフィ|LINE\s*(を)?\s*追加|プロフ(見|から|に)|フォロー(して|お願い)|DM\s*(ください|して|下さい)/,
  },
  {
    reason: "criticism",
    re: /詐欺|ステマ|嘘つき|うそつき|宣伝乙|最悪|ふざけ/,
  },
  {
    reason: "health",
    re: /うつ病|鬱|死にたい|消えたい|自殺|自傷|通院|診断(され|を受け|結果)|副作用|カウンセリング|適応障害|パニック障害|不眠症|睡眠薬|精神科|心療内科|薬を飲|病院に行/,
  },
  {
    reason: "deep_personal",
    re: /借金|離婚|死別|亡くな|虐待|家庭内暴力|リストラされた|解雇された/,
  },
];

const SOSEKI_HOLD_EXTRA = [
  { reason: "health", re: /眠れなくて困|寝つけない日|不眠で悩|眠剤/ },
];

function classifyHold(text, account) {
  for (const p of HOLD_PATTERNS) if (p.re.test(text)) return p.reason;
  if (account === "soseki") {
    for (const p of SOSEKI_HOLD_EXTRA) if (p.re.test(text)) return p.reason;
  }
  return null;
}

// Ruri solicitation posts (募集型) stay in scope for 24h instead of 3h.
function isSolicitPost(text) {
  return /教えて|聞かせて|募集|ありますか|何使っ|おすすめ|みんなは|どうしてる|知りたい|一言だけ|吐き出して/.test(
    text || ""
  );
}

// Picks one quotable clause out of a comment (the pattern sets ask for
// the commenter's own words to be picked up). Returns null when no
// clause is usable; templates have no-echo fallbacks.
function extractEcho(text) {
  if (!text) return null;
  const clauses = String(text)
    .split(/[。．！!？?\n、,]/)
    .map((s) =>
      s
        .trim()
        .replace(/^(私も|私の|自分も|うちも|ほんと|本当に|本当|それ|でも|やっぱり)/, "")
        .replace(/(かなぁ|かな|かも|よね|だよね|なぁ)$/, "")
        .trim()
    )
    .filter((s) => s.length >= 4);
  let best = null;
  for (const c of clauses) {
    if (c.length <= 22 && (!best || c.length > best.length)) best = c;
  }
  if (!best && clauses.length) {
    const first = clauses[0];
    if (first.length >= 4) best = first.slice(0, 20);
  }
  return best;
}

function hashStr(s) {
  let h = 0;
  const str = String(s || "");
  for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) >>> 0;
  return h;
}

// A permission failure (dead/revoked token, missing scope) is told
// apart from ordinary API errors by its message shape. Ordinary errors
// only fail the single comment; permission errors stop the account.
function isPermissionError(e) {
  const m = String((e && e.message) || e || "");
  return /permission|oauth|access token|invalidated|code 190/i.test(m);
}

// Soseki templates per reply-knowledge type. Voice: quiet, no emoji,
// no hashtags, no links, 1-3 sentences.
const SOSEKI_TEMPLATES = {
  1: [
    (e) =>
      e
        ? `わかります。${e}という感覚、言葉にしてもらえた気がします。`
        : `わかります。その感覚、言葉にするのは難しいですよね。`,
    (e) =>
      e
        ? `同じでした。${e}——その途中でもやもやする感じ、よく覚えています。`
        : `同じでした。割り切れないまま日々が過ぎていく感じ、よく覚えています。`,
    (e) =>
      e
        ? `うなずきながら読みました。${e}というところ、特に引っかかりました。`
        : `うなずきながら読みました。静かに効いてくる話ですよね。`,
    (e) =>
      e
        ? `それ、静かに効いてきますよね。${e}の部分、私も引っかかりました。`
        : `それ、静かに効いてきますよね。日々のことだけに、なおさらです。`,
  ],
  2: [
    (e) =>
      e
        ? `「${e}」のところ、具体的でよく伝わりました。そういう場面があったんですね。`
        : `具体的な場面が目に浮かびました。書いてくださってありがとうございます。`,
    (e) =>
      e
        ? `「${e}」——その一言に、それまでの積み重ねが出ている気がします。`
        : `その一言に、それまでの積み重ねが出ている気がします。`,
    () =>
      `読んでいて、その場面が目に浮かびました。経験を言葉にしてくださって、ありがとうございます。`,
  ],
  3: [
    () =>
      `難しいところですよね。答えは一つではないと思いますが、考えるきっかけになりました。`,
    () =>
      `いい問いだと思います。はっきりとは言えませんが、そこを考えるところから始まる気がします。`,
    (e) =>
      e
        ? `そこは、私も断定できません。ただ、${e}と感じるところに、ヒントがあるのかもしれません。`
        : `そこは、私も断定できません。だからこそ、考え続ける価値がある気がします。`,
  ],
  4: [
    () => `ありがとうございます。そう言ってもらえると、書いてよかったと思います。`,
    () => `読んでくださってありがとうございます。`,
    () => `ありがとうございます。その言葉が励みになります。`,
  ],
  5: [
    () =>
      `甘えではないと思います。頑張りすぎて消耗している状態に近いのではないでしょうか。`,
    () =>
      `それを甘えと呼ぶのは、少し違う気がします。限界まで我慢した結果の消耗、と見る方が近いと思います。`,
    () =>
      `甘えではなく、休む余裕をなくしている状態だと思います。自分を責める材料にしなくて大丈夫です。`,
  ],
};

// Ruri templates per interaction type (A empathy-only / B empathy+own
// story / C empathy+question / D empathy+view / E know-how). Voice:
// cheerful, everyday words; no product names, links or effect claims.
const RURI_TEMPLATES = {
  A: [
    (e) =>
      e
        ? `ほんとそれ〜！${e}って、ぐっと印象変わるよね✨`
        : `ほんとそれ〜！教えてくれてありがとう✨`,
    (e) =>
      e
        ? `わかる〜！${e}、ほんと大事だよね😊`
        : `わかる〜！ほんと大事なところだよね😊`,
    (e) =>
      e
        ? `うんうん、${e}って実感ある〜！`
        : `うんうん、実感ある〜！教えてくれてありがとう😊`,
  ],
  B: [
    (e) =>
      e
        ? `わかる〜！私も${e}って感じることあるよ😊 つい後回しにしちゃうんだよね…`
        : `わかる〜！私もつい後回しにしちゃうんだよね…😊 気づいたときが始めどきだと思うなぁ`,
    () =>
      `ほんとそれ〜！私も朝バタバタしてると後回しになりがちで…😅 でも大事だよね`,
    (e) =>
      e
        ? `うんうん、私も同じだよ〜。${e}って、気づいたときが始めどきだと思うなぁ😊`
        : `うんうん、私も同じだよ〜。気づいたときが始めどきだと思うなぁ😊`,
  ],
  C: [
    (e) =>
      e
        ? `わかる〜！${e}だよね😊 ちなみに、なにかきっかけがあったの？`
        : `わかる〜！ちなみに、なにかきっかけがあったの？😊`,
    () => `ほんとそれ〜！気になるよね😊 みんなはどうしてるんだろう？`,
    (e) =>
      e
        ? `うんうん、${e}だよね😊 きっかけって何だったの？`
        : `うんうん、きっかけって何だったの？😊`,
  ],
  D: [
    (e) =>
      e
        ? `わかる〜。${e}って、結局は順番と量なんだと思うなぁ😊`
        : `わかる〜。結局は順番と量なんだと思うなぁ😊`,
    () => `うんうん、高いものより、続けられる順番だと思う〜😊`,
  ],
  E: [
    () =>
      `わかる〜！基本は保湿と日焼け止めだよね😊 私もまずそこだけは守るようにしてるよ`,
    () => `ほんとそれ〜！順番をそろえるだけで、使う量も安定する気がするよ😊`,
  ],
};

function pickSosekiType(text, state) {
  let classified;
  if (!text) classified = "4";
  else if (/甘え/.test(text)) classified = "5";
  else if (
    /[？?]|どう(したら|すれば|思う|思い)|なぜ|なんで|でしょうか|ますか|いつ|どこ/.test(
      text
    )
  )
    classified = "3";
  else if (
    text.length >= 44 ||
    /私も|私の|自分も|うちも|辞めた|転職した|経験/.test(text)
  )
    classified = "2";
  else if (
    /ありがとう|ありがたい|参考にな|嬉しい|うれしい|よかった|良かった|素敵|励まされ/.test(
      text
    )
  )
    classified = "4";
  else classified = "1";
  // Self-blame comments always stay on type 5 (only the wording rotates).
  if (classified === "5") return "5";
  const prefs = {
    1: ["1", "2", "4"],
    2: ["2", "1", "4"],
    3: ["3", "1", "4"],
    4: ["4", "1", "2"],
  };
  const lastType = state.last && state.last.type;
  for (const t of prefs[classified]) if (t !== lastType) return t;
  return classified;
}

function pickRuriType(text, state, commentId) {
  let classified;
  if (!text) classified = "A";
  else if (/何使って|おすすめ(を)?(教えて|ありますか)|教えて(ください|！|!)/.test(text))
    classified = "C"; // product-asking: warm question back, never a recommendation
  else if (/保湿|日焼け止め|順番|洗顔|クレンジング|化粧水|美容液|シャンプー/.test(text))
    classified = "E";
  else if (
    /ありがとう|参考にな|嬉しい|うれしい|助かる|すごい|好き|よかった|良かった/.test(text)
  )
    classified = "A";
  else if (
    /私も|私の|自分も|うちも|なっちゃう|でした|だった/.test(text) ||
    text.length >= 30
  )
    classified = "B";
  else if (/[？?]/.test(text)) classified = "C";
  else classified = "A";
  // Type E (know-how) is capped at 1 in the last 5 replies.
  if (classified === "E" && (state.recent_types || []).includes("E")) {
    classified = "B";
  }
  // Type D (a view, occasionally) for opinion-like comments.
  if (
    classified === "A" &&
    text &&
    /思う|感じる/.test(text) &&
    !(state.recent_types || []).includes("D") &&
    hashStr(commentId) % 3 === 0
  ) {
    classified = "D";
  }
  const prefs = {
    A: ["A", "B", "C"],
    B: ["B", "A", "C"],
    C: ["C", "A", "B"],
    D: ["D", "A", "B"],
    E: ["E", "B", "A"],
  };
  const lastType = state.last && state.last.type;
  for (const t of prefs[classified]) if (t !== lastType) return t;
  return classified;
}

// Builds one reply text from the pattern set, honouring the rotation
// rule: the opening (first 6 chars) must differ from the previous
// reply's opening; the variant choice is deterministic per comment.
function buildReply(account, type, echo, state, commentId) {
  const variants =
    (account === "soseki" ? SOSEKI_TEMPLATES : RURI_TEMPLATES)[type] || [];
  const cands = variants
    .map((make) => make(echo))
    .filter((t) => t && t.length <= 220);
  const lastOpen = state.last && state.last.opening;
  let pool = cands.filter((t) => t.slice(0, 6) !== lastOpen);
  if (!pool.length) pool = cands;
  const text = pool[hashStr(commentId) % pool.length];
  return { type, opening: text.slice(0, 6), text };
}

async function runCommentAccount(env, account, tokenKey, { dry = false } = {}) {
  const s = {
    username: null,
    posts_checked: 0,
    new_comments: 0,
    replied: [],
    holds: [],
    would_reply: [],
    errors: [],
    pending: 0,
    perm_error: false,
  };

  const token = await env.STATE.get(tokenKey);
  if (!token) {
    s.errors.push(`kv token missing: ${tokenKey}`);
    return s;
  }

  const stateKey = `comment-state:${account}`;
  let state = { seen: {}, last: null, recent_types: [], perm_error: null };
  try {
    const raw = await env.STATE.get(stateKey);
    if (raw) state = { ...state, ...JSON.parse(raw) };
  } catch (_) {
    // unreadable state: start fresh rather than fail the lane
  }
  if (!state.seen || typeof state.seen !== "object") state.seen = {};

  // Permission stop: once a permission error is recorded, no further
  // reply attempts are made until the KV token is replaced (a new
  // token has a different fingerprint and clears the stop by itself).
  const fp = String(hashStr(token));
  if (state.perm_error) {
    if (state.perm_error.fp === fp) {
      s.perm_error = true;
      s.errors.push("要対応：権限エラー (comment replies stopped until re-authorization)");
      return s;
    }
    state.perm_error = null;
  }

  const persist = async () => {
    if (dry) return;
    const cutoff = Date.now() - COMMENT_LANE.stateKeepMs;
    for (const [id, v] of Object.entries(state.seen)) {
      if (!v || !v.at || Date.parse(v.at) < cutoff) delete state.seen[id];
    }
    await env.STATE.put(stateKey, JSON.stringify(state));
  };

  let me;
  try {
    me = await threadsFetch("/me?fields=id,username", token);
  } catch (e) {
    if (e instanceof RateLimitError) {
      s.errors.push("rate_limited");
      return s;
    }
    if (isPermissionError(e)) {
      state.perm_error = {
        at: new Date().toISOString(),
        fp,
        error: String((e && e.message) || e).slice(0, 200),
      };
      s.perm_error = true;
      s.errors.push("要対応：権限エラー");
      await persist();
      return s;
    }
    s.errors.push(String((e && e.message) || e).slice(0, 200));
    return s;
  }
  s.username = me.username || null;

  let posts = [];
  try {
    const r = await threadsFetch(
      `/me/threads?fields=id,text,permalink,timestamp&limit=${COMMENT_LANE.postsLimit}`,
      token
    );
    posts = r.data || [];
  } catch (e) {
    if (e instanceof RateLimitError) {
      s.errors.push("rate_limited");
      return s;
    }
    s.errors.push(String((e && e.message) || e).slice(0, 200));
    return s;
  }

  const now = Date.now();
  const inWindow = posts.filter((p) => {
    const t = Date.parse(p.timestamp || "");
    if (!Number.isFinite(t)) return false;
    const age = now - t;
    if (age <= COMMENT_LANE.windowMs) return true;
    return (
      account === "ruri" &&
      age <= COMMENT_LANE.ruriSolicitWindowMs &&
      isSolicitPost(p.text)
    );
  });
  s.posts_checked = inWindow.length;

  const candidates = [];
  for (const post of inWindow) {
    let reps;
    try {
      const r = await threadsFetch(
        `/${post.id}/replies?fields=id,text,username,timestamp,permalink,media_type`,
        token
      );
      reps = r.data || [];
    } catch (e) {
      if (e instanceof RateLimitError) {
        s.errors.push("rate_limited");
        break;
      }
      s.errors.push(
        `replies fetch ${post.id}: ${String((e && e.message) || e).slice(0, 150)}`
      );
      continue;
    }
    for (const c of reps) {
      if (!c.id || c.username === me.username) continue;
      const prev = state.seen[c.id];
      if (prev && prev.s !== "error") continue;
      if (prev && prev.s === "error" && (prev.attempts || 0) >= 2) continue;
      candidates.push({ comment: c, post });
    }
  }
  candidates.sort(
    (a, b) =>
      Date.parse(a.comment.timestamp || "") - Date.parse(b.comment.timestamp || "")
  );
  s.new_comments = candidates.length;

  let repliesDone = 0;
  let processed = 0;
  for (const { comment, post } of candidates) {
    if (processed >= COMMENT_LANE.maxCandidatesPerRun) break;
    processed++;
    const text = (comment.text || "").trim();

    const hold = text ? classifyHold(text, account) : null;
    if (hold) {
      state.seen[comment.id] = {
        s: "hold",
        reason: hold,
        at: new Date().toISOString(),
        post_id: post.id,
      };
      s.holds.push({
        comment_id: comment.id,
        reason: hold,
        author: comment.username || null,
      });
      continue;
    }

    if (repliesDone >= COMMENT_LANE.maxRepliesPerRun) continue;

    const echo = extractEcho(text);
    const type =
      account === "soseki"
        ? pickSosekiType(text, state)
        : pickRuriType(text, state, comment.id);
    const tpl = buildReply(account, type, echo, state, comment.id);

    if (dry) {
      s.would_reply.push({ comment_id: comment.id, type: tpl.type, text: tpl.text });
      repliesDone++;
      continue;
    }

    try {
      const { postId, permalink } = await postToThreads(
        me,
        token,
        tpl.text,
        comment.id
      );
      state.seen[comment.id] = {
        s: "replied",
        at: new Date().toISOString(),
        post_id: post.id,
        reply_id: postId,
        type: tpl.type,
      };
      state.last = { type: tpl.type, opening: tpl.opening };
      state.recent_types = [...(state.recent_types || []), tpl.type].slice(-5);
      await env.STATE.put(
        `reply:${account}:${comment.id}`,
        JSON.stringify({
          account,
          comment_id: comment.id,
          comment_text: text.slice(0, 300),
          author: comment.username || null,
          on_post_id: post.id,
          on_post_permalink: post.permalink || null,
          type: tpl.type,
          reply_text: tpl.text,
          reply_id: postId,
          permalink,
          at: new Date().toISOString(),
        })
      );
      s.replied.push({ comment_id: comment.id, type: tpl.type, permalink });
      repliesDone++;
    } catch (e) {
      if (e instanceof RateLimitError) {
        s.errors.push("rate_limited");
        break;
      }
      if (isPermissionError(e)) {
        state.perm_error = {
          at: new Date().toISOString(),
          fp,
          error: String((e && e.message) || e).slice(0, 200),
        };
        s.perm_error = true;
        s.errors.push("要対応：権限エラー");
        break;
      }
      const prev = state.seen[comment.id];
      state.seen[comment.id] = {
        s: "error",
        attempts: ((prev && prev.attempts) || 0) + 1,
        at: new Date().toISOString(),
        error: String((e && e.message) || e).slice(0, 200),
      };
      s.errors.push({
        comment_id: comment.id,
        error: String((e && e.message) || e).slice(0, 200),
      });
    }
  }
  const errored = s.errors.filter((e) => e && e.comment_id).length;
  s.pending = Math.max(
    0,
    candidates.length - (s.replied.length + s.holds.length + s.would_reply.length + errored)
  );

  await persist();
  return s;
}

async function runCommentLane(env, { dry = false } = {}) {
  const out = {};
  for (const { account, tokenKey } of COMMENT_LANE.accounts) {
    try {
      out[account] = await runCommentAccount(env, account, tokenKey, { dry });
    } catch (e) {
      out[account] = {
        username: null,
        posts_checked: 0,
        new_comments: 0,
        replied: [],
        holds: [],
        would_reply: [],
        errors: [String((e && e.message) || e).slice(0, 300)],
        pending: 0,
        perm_error: false,
      };
    }
  }
  return out;
}

async function run(env, { dry = false } = {}) {
  const startedAt = new Date().toISOString();
  const summary = {
    at: startedAt,
    dry,
    accounts: {},
    errors: [],
    notion: { enabled: !!env.NOTION_TOKEN, fetched: 0, errors: [] },
    comments: null,
  };

  const qResp = await fetch(`${CONFIG.queueUrl}?t=${Date.now()}`, {
    headers: { "User-Agent": "social-auto-poster" },
    cf: { cacheTtl: 0, cacheEverything: false },
  });
  if (!qResp.ok) {
    summary.errors.push(`queue fetch http ${qResp.status}`);
    return summary;
  }
  const queue = await qResp.json();
  if (!Array.isArray(queue.items)) queue.items = [];

  // Merge the Notion queue source. A Notion outage must never break
  // GitHub queue processing, so failures only land in summary.notion.
  // GitHub items win on item_id collisions.
  try {
    const notion = await fetchNotionQueue(env);
    summary.notion.enabled = notion.enabled;
    summary.notion.fetched = notion.items.length;
    summary.notion.errors.push(...notion.errors);
    const knownIds = new Set(queue.items.map((i) => i.item_id));
    for (const item of notion.items) {
      if (knownIds.has(item.item_id)) continue;
      queue.items.push(item);
      knownIds.add(item.item_id);
    }
  } catch (e) {
    summary.notion.errors.push(
      String((e && e.message) || e).slice(0, 300)
    );
  }

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

  // Comment auto-reply lane (soseki + ruri). Isolated on purpose: a
  // failure here must never affect the posting lanes above.
  try {
    summary.comments = await runCommentLane(env, { dry });
  } catch (e) {
    summary.comments = {
      error: String((e && e.message) || e).slice(0, 300),
    };
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