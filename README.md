# social-auto-poster

承認済み投稿を、ユーザーの Cloudflare 基盤から公式APIで自動投稿するプログラム。
Phase 1 は るり（@ruri_40s_biyou）の Threads 投稿パイロット。

## 構成

- `worker/worker.js` — Cloudflare Worker 本体。Cron Trigger（30分毎）で起動し、
  このリポジトリの `queue.json`（raw URL）を読み、投稿時刻を過ぎた未投稿分を
  Threads API で投稿する。結果は Cloudflare Workers KV（namespace `auto-poster`）の
  `result:<item_id>` に記録され、二重投稿を防ぐ。429 の場合はその回の処理を打ち切る。
  失敗は次回 Cron で再試行し、最大3回まで。
- `queue.json` — 承認済み投稿のキュー。`item_id` は一意ID、`source_cron_id` は
  移行元になった Muse 側 cron ジョブのID。文面は承認済みのものをそのまま入れる。
- `wrangler.toml` — Worker 設定（名前・Cron・KV バインディング）。

## 秘密情報

- Threads の長期トークンは Cloudflare Workers KV（namespace `auto-poster`、
  キー `token:threads:ruri`）にだけ置く。このリポジトリには入れない。
- このリポジトリが public なのは、Worker が認証なしで `queue.json` を raw URL から
  読むため。キューには公開予定の投稿文面しか入っていない。
  private 化する場合は、Worker に GitHub の fine-grained PAT（このリポジトリの
  contents 読み取りのみ）を Secret として渡し、読み取りを GitHub API 経由に
  切り替える必要がある。

## 確認用エンドポイント（読み取りのみ）

- `/__status` — 直近の Cron 実行サマリ（KV の `state:last_run`）
- `/__dryrun` — トークンの本人確認とキューの読み取り・時刻判定のみ（投稿しない）

## デプロイ

`wrangler deploy`、または Cloudflare API でスクリプトをアップロードし、
スケジュール `*/30 * * * *` を設定する（デプロイ済みの状態が正）。
