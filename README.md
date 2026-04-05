# Anthropic Usage Stats Collector

A lightweight cron-scheduled tool that pulls usage and cost data from the Anthropic Admin API and stores it in a local SQLite database for reporting.

## Requirements

- Python 3.10+
- An Anthropic Admin API key (`sk-ant-admin...`)
- Organization membership with admin role

## Setup

```bash
pip install -r requirements.txt

# Set your admin API key
export ANTHROPIC_ADMIN_API_KEY="sk-ant-admin-..."
```

## Usage

### Collect data (default mode)

```bash
# Fetch last 25 hours of data with 1h buckets
python3 -m src.collect

# Custom lookback and bucket width
python3 -m src.collect --lookback 48 --bucket-width 1d

# Dry run — fetch and print without writing to DB
python3 -m src.collect --dry-run
```

### Set plan limits

```bash
python3 -m src.collect --set-limits \
  --daily-token-limit 1000000 \
  --daily-cost-limit 500 \
  --weekly-token-limit 5000000 \
  --weekly-cost-limit 2500
```

### Generate reports

```bash
python3 -m src.collect --report weekly
python3 -m src.collect --report daily
python3 -m src.collect --report overage
```

### Scheduling with Cron

Cron is a built-in macOS/Linux scheduler that runs commands on a repeating schedule. Each line in your crontab defines a job with a time pattern and a command.

#### Cron expression format

```
┌───────────── minute (0–59)
│ ┌─────────── hour (0–23)
│ │ ┌───────── day of month (1–31)
│ │ │ ┌─────── month (1–12)
│ │ │ │ ┌───── day of week (0–6, Sun=0)
│ │ │ │ │
* * * * *  command
```

#### How to set up

1. Open your crontab in the terminal:
   ```bash
   crontab -e
   ```

2. Add one of the following lines (replace the path and API key with yours):

   ```bash
   # Hourly collection (recommended) — runs at the top of every hour
   0 * * * * cd /path/to/anthropic-usage-storage && ANTHROPIC_ADMIN_API_KEY="sk-ant-admin-..." python3 -m src.collect >> /var/log/anthropic-usage.log 2>&1

   # Daily collection — runs at 2:15 AM with daily buckets and 48h lookback
   15 2 * * * cd /path/to/anthropic-usage-storage && ANTHROPIC_ADMIN_API_KEY="sk-ant-admin-..." python3 -m src.collect --bucket-width 1d --lookback 48 >> /var/log/anthropic-usage.log 2>&1
   ```

3. Save and exit. Verify your crontab was saved:
   ```bash
   crontab -l
   ```

#### Exit code alerting

The tool returns exit code 2 when plan limits are exceeded. You can capture this in your cron job to create a separate alert log:

```bash
0 * * * * cd /path/to/anthropic-usage-storage && ANTHROPIC_ADMIN_API_KEY="sk-ant-admin-..." python3 -m src.collect >> /var/log/anthropic-usage.log 2>&1 || echo "$(date): Usage alert exit code $?" >> /var/log/anthropic-alerts.log
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (missing API key, API failure, etc.) |
| 2 | Limits exceeded (for cron alerting) |

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ANTHROPIC_ADMIN_API_KEY` | Admin API key (required for collection) | — |
| `USAGE_DB_PATH` | SQLite database path | `./usage.db` |
| `LOOKBACK_HOURS` | Hours of history to fetch | `25` |

## Project structure

```
src/
  db.py        — SQLite schema + upsert/query functions
  api.py       — Anthropic Admin API client with pagination
  limits.py    — Plan limit checking and warnings
  reports.py   — Formatted report generation
  collect.py   — CLI entry point
tests/
  test_db.py
  test_api.py
  test_limits.py
  test_reports.py
  test_collect.py
```

## Running tests

```bash
python3 -m pytest tests/ -v
```
