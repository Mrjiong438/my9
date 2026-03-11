# Share Storage V2 Ops

## New env vars

- `R2_ENDPOINT`
- `R2_BUCKET`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- Optional: `R2_REGION` (default: `auto`)
- Optional: `MY9_ENABLE_V1_FALLBACK=0` (default keeps `my9_shares_v1` read fallback)
- Optional: `CRON_SECRET` (recommended in production, used by Vercel Cron Authorization header)
- Optional: `MY9_ARCHIVE_OLDER_THAN_DAYS` (default `30`)
- Optional: `MY9_ARCHIVE_BATCH_SIZE` (default `500`)
- Optional: `MY9_ARCHIVE_CLEANUP_TREND_DAYS` (default `190`)

## Migration

Run idempotent migration with checkpoint:

```bash
node scripts/migrate-shares-v1-to-v2.mjs
```

Useful flags:

- `node scripts/migrate-shares-v1-to-v2.mjs --batch-size=300`
- `node scripts/migrate-shares-v1-to-v2.mjs --max-rows=5000`

Checkpoint file: `scripts/.migrate-shares-v1.checkpoint.json`

## Migration verify

Run migration consistency checks (`old`, `v2`, `alias`, `missing`):

```bash
node scripts/verify-shares-v2-migration.mjs
```

## Trend table rebuild (subject-grain)

Current trend tables are:

- `my9_trend_subject_all_v2`
- `my9_trend_subject_day_v2`

They only store `subject_id + count` (no `kind/view/bucket`) to reduce write amplification and table size.

Rebuild from old trend tables and drop old heavy tables:

```bash
node scripts/rebuild-trends-subject-v2.mjs
```

Optional flag:

- `node scripts/rebuild-trends-subject-v2.mjs --reset` (truncate new trend tables before rebuild)

## DB usage monitor

```bash
node scripts/monitor-db-usage.mjs
```

Useful flags:

- `node scripts/monitor-db-usage.mjs --json`
- `node scripts/monitor-db-usage.mjs --max-mb=512 --warn-percent=70 --critical-percent=90`
- `node scripts/monitor-db-usage.mjs --top=15`
- `node scripts/monitor-db-usage.mjs --fail-on=warn` or `--fail-on=critical`
- `node scripts/monitor-db-usage.mjs --exact-counts` (slower, full table count)

## Cold archive + day-count cleanup

```bash
node scripts/archive-shares-cold.mjs
```

Useful flags:

- `node scripts/archive-shares-cold.mjs --older-than-days=30`
- `node scripts/archive-shares-cold.mjs --batch-size=500`
- `node scripts/archive-shares-cold.mjs --cleanup-trend-days=190`

## Vercel Cron (daily, Hobby-safe)

- Cron route: `/api/cron/archive`
- Config file: `vercel.json`
- Current schedule: `0 3 * * *` (UTC, once per day)
- Route default behavior: archive shares older than `30` days

Notes from Vercel docs for Hobby:

- Minimum cron interval on Hobby is once per day.
- Failed runs are not retried automatically. Check logs and re-run manually when needed.

Recommended setup:

1. Set `CRON_SECRET` in Vercel project env.
2. Redeploy so `vercel.json` cron is applied.
3. Verify route manually once:
   ```bash
   curl -H "Authorization: Bearer <CRON_SECRET>" https://<your-domain>/api/cron/archive
   ```
