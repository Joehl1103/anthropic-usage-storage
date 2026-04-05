## Anthropic Usage Stats Collector

A lightweight cron-scheduled tool that pulls usage and cost data from the Anthropic Admin API and stores it in a local SQLite database for reporting.

---

### Overview

The Anthropic Admin API exposes two key endpoints for tracking consumption:
- `GET /v1/organizations/usage_report/messages` — token-level usage data
- `GET /v1/organizations/cost_report` — USD cost breakdowns

This tool would run on a schedule (e.g. hourly or daily via cron), pull the latest data, and append it to a SQLite database. This enables historical trend analysis, cost alerting, and custom dashboards without relying on third-party SaaS.

---

### Authentication

- **Requires an Admin API Key** (prefix `sk-ant-admin...`)
- Only org members with the **admin** role can generate these in the Claude Console → Settings → Admin Keys
- Standard API keys will NOT work for these endpoints
- The tool reads the key from an environment variable: `ANTHROPIC_ADMIN_API_KEY`

---

### Data Collected

#### Usage Report (`/v1/organizations/usage_report/messages`)

| Field | Description |
|-------|-------------|
| `uncached_input_tokens` | Input tokens without cache hits |
| `cache_read_input_tokens` | Input tokens served from cache |
| `cache_creation.ephemeral_1h_input_tokens` | Tokens cached for 1 hour |
| `cache_creation.ephemeral_5m_input_tokens` | Tokens cached for 5 minutes |
| `output_tokens` | Tokens generated |
| `server_tool_use.web_search_requests` | Web search tool invocations |
| `model` | Claude model used |
| `api_key_id` | API key used (null for Console/Workbench) |
| `workspace_id` | Workspace identifier |
| `service_tier` | standard, batch, priority, flex, etc. |
| `context_window` | 0-200k or 200k-1M |
| `inference_geo` | global, us, or not_available |
| `starting_at` / `ending_at` | Time bucket boundaries (RFC 3339 UTC) |

#### Cost Report (`/v1/organizations/cost_report`)

| Field | Description |
|-------|-------------|
| `amount` | Cost in lowest currency units (cents as decimal string) |
| `currency` | Always USD |
| `cost_type` | tokens, web_search, or code_execution |
| `token_type` | uncached_input, output, cache_read, cache_creation variants |
| `model` | Model used |
| `service_tier` | standard or batch |
| `description` | Human-readable cost description |

---

### Proposed Tech Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Python 3.10+ | Simple scripting, good HTTP/DB libs |
| HTTP Client | `httpx` or `requests` | Straightforward API calls |
| Database | **SQLite** (via `sqlite3` stdlib) | Zero-dependency, file-based, perfect for single-node reporting |
| Scheduling | System cron or `systemd` timer | No runtime daemon needed |
| Config | Environment variables + optional `.env` file | 12-factor style |

---

### SQLite Schema (Proposed)

```sql
-- Token-level usage snapshots
CREATE TABLE usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    bucket_start TEXT NOT NULL,
    bucket_end TEXT NOT NULL,
    model TEXT,
    workspace_id TEXT,
    api_key_id TEXT,
    service_tier TEXT,
    context_window TEXT,
    inference_geo TEXT,
    uncached_input_tokens INTEGER DEFAULT 0,
    cache_read_input_tokens INTEGER DEFAULT 0,
    cache_creation_1h_tokens INTEGER DEFAULT 0,
    cache_creation_5m_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    web_search_requests INTEGER DEFAULT 0,
    UNIQUE(bucket_start, bucket_end, model, workspace_id, api_key_id, service_tier)
);

-- Cost snapshots
CREATE TABLE cost_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    bucket_start TEXT NOT NULL,
    bucket_end TEXT NOT NULL,
    model TEXT,
    workspace_id TEXT,
    service_tier TEXT,
    cost_type TEXT,
    token_type TEXT,
    amount_cents REAL NOT NULL,
    currency TEXT DEFAULT 'USD',
    description TEXT,
    UNIQUE(bucket_start, bucket_end, model, workspace_id, service_tier, cost_type, token_type)
);

-- Plan limits for tracking daily window and overage
CREATE TABLE plan_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_date TEXT NOT NULL DEFAULT (date('now')),
    daily_token_limit INTEGER,          -- max tokens per day (input + output)
    daily_cost_limit_cents INTEGER,     -- max daily spend in cents
    weekly_token_limit INTEGER,         -- max tokens per week
    weekly_cost_limit_cents INTEGER,    -- max weekly spend in cents
    notes TEXT,
    UNIQUE(effective_date)
);

-- Lightweight run log for debugging
CREATE TABLE sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    status TEXT NOT NULL,  -- success | error
    records_fetched INTEGER DEFAULT 0,
    error_message TEXT
);
```

---

### Tool Behavior

1. **Startup**: Read config from env vars (`ANTHROPIC_ADMIN_API_KEY`, `USAGE_DB_PATH`, `LOOKBACK_HOURS`)
2. **Fetch usage**: Call `/v1/organizations/usage_report/messages` with `group_by=[model, workspace_id, api_key_id, service_tier]` for the lookback window, paginating if `has_more` is true
3. **Fetch costs**: Call `/v1/organizations/cost_report` with `group_by=[model, workspace_id, description]` for the same window
4. **Upsert**: Insert records into SQLite using `INSERT OR REPLACE` on the unique constraints (idempotent — safe to re-run)
5. **Check limits**: If `plan_limits` are configured, compare today's running totals against the daily/weekly caps and print warnings to stderr if thresholds are exceeded (≥80%, ≥100%)
6. **Log**: Write a row to `sync_log` with status and record count
7. **Report** (optional): If `--report` is passed, run the corresponding query set and print results as a formatted table to stdout, then exit
8. **Exit**: Return exit code 0 on success, 1 on failure, 2 if limits exceeded (for cron alerting)

---

### API Rate Limit Considerations

- Anthropic recommends polling **no more than once per minute** for sustained use
- Data freshness: usage data typically appears within **5 minutes** of request completion
- Pagination: responses include `has_more` and `next_page` fields — the tool must follow these
- Bucket width options: `1m` (max 1440 buckets), `1h` (max 168), `1d` (max 31)
- **Recommended default**: `1h` buckets with a 25-hour lookback (overlapping window ensures no gaps)

---

### Cron Configuration Examples

```bash
# Hourly pull (recommended)
0 * * * * cd /opt/anthropic-usage-stats && python collect.py >> /var/log/anthropic-usage.log 2>&1

# Daily pull (if hourly granularity not needed)
15 2 * * * cd /opt/anthropic-usage-stats && python collect.py --bucket-width 1d --lookback 48 >> /var/log/anthropic-usage.log 2>&1
```

---

### CLI Interface (Proposed)

```
usage: collect.py [-h] [--db PATH] [--lookback HOURS] [--bucket-width {1m,1h,1d}] [--dry-run]
                  [--set-limits] [--report {weekly,daily,overage}]

Options:
  --db PATH              SQLite database path (default: ./usage.db, or $USAGE_DB_PATH)
  --lookback HOURS       Hours of history to fetch (default: 25)
  --bucket-width         Time granularity: 1m, 1h, 1d (default: 1h)
  --dry-run              Fetch and print data without writing to DB
  --set-limits           Set plan limits interactively or via flags below
    --daily-token-limit N        Daily token cap
    --daily-cost-limit CENTS     Daily cost cap in cents
    --weekly-token-limit N       Weekly token cap
    --weekly-cost-limit CENTS    Weekly cost cap in cents
  --report TYPE          Generate a report: weekly, daily, or overage
```

---

### Reporting / Query Examples

Once data is collected, users can query directly with SQLite or connect any tool that speaks SQLite (Grafana, Metabase, Datasette, etc.).

#### Weekly Usage Report

Aggregated token and cost totals per ISO week, broken down by model.

```sql
-- Weekly token usage by model
SELECT strftime('%Y-W%W', bucket_start) AS week,
       model,
       SUM(uncached_input_tokens) AS uncached_input,
       SUM(cache_read_input_tokens) AS cache_read,
       SUM(output_tokens) AS output,
       SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens) AS total_tokens
FROM usage_records
WHERE bucket_start >= date('now', '-28 days')
GROUP BY week, model
ORDER BY week DESC, total_tokens DESC;

-- Weekly cost summary
SELECT strftime('%Y-W%W', bucket_start) AS week,
       SUM(amount_cents) / 100.0 AS total_usd
FROM cost_records
WHERE bucket_start >= date('now', '-28 days')
GROUP BY week
ORDER BY week DESC;
```

#### Daily Window Usage (Allocation Tracking)

Shows how much of each day's configured limit has been consumed. Requires a row in `plan_limits` for the current period.

```sql
-- Daily usage vs. plan limit (tokens)
SELECT u.day,
       u.total_tokens,
       p.daily_token_limit,
       CASE
           WHEN p.daily_token_limit IS NOT NULL
           THEN ROUND(u.total_tokens * 100.0 / p.daily_token_limit, 1)
           ELSE NULL
       END AS pct_used
FROM (
    SELECT date(bucket_start) AS day,
           SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens) AS total_tokens
    FROM usage_records
    WHERE bucket_start >= date('now', '-7 days')
    GROUP BY day
) u
LEFT JOIN plan_limits p
    ON p.effective_date = (
        SELECT MAX(effective_date) FROM plan_limits WHERE effective_date <= u.day
    )
ORDER BY u.day DESC;

-- Daily cost vs. plan limit
SELECT c.day,
       ROUND(c.total_cents / 100.0, 2) AS spent_usd,
       ROUND(p.daily_cost_limit_cents / 100.0, 2) AS limit_usd,
       CASE
           WHEN p.daily_cost_limit_cents IS NOT NULL
           THEN ROUND(c.total_cents * 100.0 / p.daily_cost_limit_cents, 1)
           ELSE NULL
       END AS pct_used
FROM (
    SELECT date(bucket_start) AS day, SUM(amount_cents) AS total_cents
    FROM cost_records
    WHERE bucket_start >= date('now', '-7 days')
    GROUP BY day
) c
LEFT JOIN plan_limits p
    ON p.effective_date = (
        SELECT MAX(effective_date) FROM plan_limits WHERE effective_date <= c.day
    )
ORDER BY c.day DESC;
```

#### Extra / Overage Usage

Surfaces days or weeks where usage exceeded the configured plan limits.

```sql
-- Days with token overage
SELECT u.day,
       u.total_tokens,
       p.daily_token_limit,
       u.total_tokens - p.daily_token_limit AS overage_tokens
FROM (
    SELECT date(bucket_start) AS day,
           SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens) AS total_tokens
    FROM usage_records
    GROUP BY day
) u
JOIN plan_limits p
    ON p.effective_date = (
        SELECT MAX(effective_date) FROM plan_limits WHERE effective_date <= u.day
    )
WHERE p.daily_token_limit IS NOT NULL
  AND u.total_tokens > p.daily_token_limit
ORDER BY u.day DESC;

-- Days with cost overage
SELECT c.day,
       ROUND(c.total_cents / 100.0, 2) AS spent_usd,
       ROUND(p.daily_cost_limit_cents / 100.0, 2) AS limit_usd,
       ROUND((c.total_cents - p.daily_cost_limit_cents) / 100.0, 2) AS overage_usd
FROM (
    SELECT date(bucket_start) AS day, SUM(amount_cents) AS total_cents
    FROM cost_records
    GROUP BY day
) c
JOIN plan_limits p
    ON p.effective_date = (
        SELECT MAX(effective_date) FROM plan_limits WHERE effective_date <= c.day
    )
WHERE p.daily_cost_limit_cents IS NOT NULL
  AND c.total_cents > p.daily_cost_limit_cents
ORDER BY c.day DESC;

-- Weekly overage summary
SELECT w.week,
       w.total_tokens,
       p.weekly_token_limit,
       w.total_tokens - p.weekly_token_limit AS overage_tokens,
       ROUND(w.total_cents / 100.0, 2) AS spent_usd,
       ROUND(p.weekly_cost_limit_cents / 100.0, 2) AS limit_usd
FROM (
    SELECT strftime('%Y-W%W', bucket_start) AS week,
           MIN(date(bucket_start)) AS week_start,
           SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens) AS total_tokens,
           0 AS total_cents
    FROM usage_records
    GROUP BY week
) w
JOIN plan_limits p
    ON p.effective_date = (
        SELECT MAX(effective_date) FROM plan_limits WHERE effective_date <= w.week_start
    )
WHERE p.weekly_token_limit IS NOT NULL
  AND w.total_tokens > p.weekly_token_limit
ORDER BY w.week DESC;
```

#### General Summaries

```sql
-- Most expensive models (all time)
SELECT model, SUM(amount_cents) / 100.0 AS total_usd
FROM cost_records
GROUP BY model
ORDER BY total_usd DESC;
```

---

### Future Enhancements (Out of Scope for v1)

- [ ] Threshold-based cost alerting (email/Slack webhook when daily spend exceeds $X)
- [ ] Datasette or Streamlit dashboard for visualization
- [ ] Docker container for portable deployment
- [ ] Support for multiple organizations
- [ ] CSV/JSON export command

---

### Important Notes

- **Priority Tier costs** use a different billing model and are NOT included in the cost endpoint — track via `service_tier=priority` in the usage endpoint instead
- **Code execution costs** appear only in the cost endpoint under `cost_type=code_execution`
- **Workbench/Console usage** has `api_key_id=null` since it is not tied to an API key
- **Individual accounts** cannot use the Admin API — an organization is required
