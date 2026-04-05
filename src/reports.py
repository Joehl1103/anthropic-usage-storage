"""
Report generation for usage, cost, and overage analytics.

This module provides functions to:
- Generate weekly usage and cost reports
- Generate daily usage and cost reports with limit comparisons
- Generate overage reports for limit violations
- Format data into human-readable tables
"""

import sqlite3
from typing import Optional


def _format_table(headers: list[str], rows: list[list]) -> str:
    """
    Format headers and rows into an aligned ASCII table.

    Creates a simple text table with columns separated by spaces and
    aligned to the longest value in each column.

    Args:
        headers: List of column header strings
        rows: List of rows, each row is a list of values

    Returns:
        A formatted string with the table, ready to print
    """
    if not headers:
        return ""

    if not rows:
        # Just return headers if no rows
        return " | ".join(str(h) for h in headers)

    # Calculate column widths based on headers and data
    col_widths = []
    for i, header in enumerate(headers):
        width = len(str(header))
        # Check all rows for this column
        for row in rows:
            if i < len(row):
                width = max(width, len(str(row[i])))
        col_widths.append(width)

    # Format header row
    header_row = " | ".join(
        str(h).ljust(col_widths[i]) for i, h in enumerate(headers)
    )

    # Format separator
    separator = "-+-".join("-" * width for width in col_widths)

    # Format data rows
    data_rows = []
    for row in rows:
        formatted_row = " | ".join(
            str(row[i]).ljust(col_widths[i]) if i < len(row) else "".ljust(col_widths[i])
            for i in range(len(headers))
        )
        data_rows.append(formatted_row)

    # Combine all parts
    lines = [header_row, separator] + data_rows
    return "\n".join(lines)


def report_weekly(conn: sqlite3.Connection) -> str:
    """
    Generate a weekly usage and cost report for the last 28 days.

    Executes two SQL queries:
    1. Weekly token usage grouped by model
    2. Weekly cost summary

    Formats results as text tables.

    Args:
        conn: sqlite3.Connection

    Returns:
        A formatted string report ready to print
    """
    cursor = conn.cursor()

    # Query 1: Weekly token usage by model
    cursor.execute(
        """
    SELECT strftime('%Y-W%W', bucket_start) AS week,
           model,
           SUM(uncached_input_tokens) AS uncached_input,
           SUM(cache_read_input_tokens) AS cache_read,
           SUM(output_tokens) AS output,
           SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens) AS total_tokens
    FROM usage_records
    WHERE bucket_start >= date('now', '-28 days')
    GROUP BY week, model
    ORDER BY week DESC, total_tokens DESC
    """
    )

    usage_rows = cursor.fetchall()

    # Format usage table
    usage_headers = ["Week", "Model", "Uncached Input", "Cache Read", "Output", "Total Tokens"]
    usage_table = _format_table(
        usage_headers,
        [[str(v) if v is not None else "" for v in row] for row in usage_rows],
    )

    # Query 2: Weekly cost summary
    cursor.execute(
        """
    SELECT strftime('%Y-W%W', bucket_start) AS week,
           SUM(amount_cents) / 100.0 AS total_usd
    FROM cost_records
    WHERE bucket_start >= date('now', '-28 days')
    GROUP BY week
    ORDER BY week DESC
    """
    )

    cost_rows = cursor.fetchall()

    # Format cost table
    cost_headers = ["Week", "Total USD"]
    cost_table = _format_table(
        cost_headers,
        [[str(v) if v is not None else "" for v in row] for row in cost_rows],
    )

    # Combine report sections
    report = f"WEEKLY USAGE REPORT (Last 28 Days)\n\n{usage_table}\n\nWEEKLY COST REPORT\n\n{cost_table}"

    return report


def report_daily(conn: sqlite3.Connection) -> str:
    """
    Generate a daily usage and cost report for the last 7 days.

    Shows daily totals for tokens and costs, optionally compared against
    configured plan limits.

    Args:
        conn: sqlite3.Connection

    Returns:
        A formatted string report ready to print
    """
    cursor = conn.cursor()

    # Query: Daily totals for the last 7 days
    cursor.execute(
        """
    SELECT DATE(bucket_start) AS day,
           SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens) AS total_tokens,
           (SELECT SUM(amount_cents) FROM cost_records WHERE DATE(cost_records.bucket_start) = DATE(usage_records.bucket_start)) AS total_cost_cents
    FROM usage_records
    WHERE bucket_start >= date('now', '-7 days')
    GROUP BY DATE(bucket_start)
    ORDER BY day DESC
    """
    )

    rows = cursor.fetchall()

    # Format as table
    headers = ["Day", "Total Tokens", "Total Cost (USD)"]
    formatted_rows = []
    for row in rows:
        day = row[0] if row[0] else ""
        tokens = int(row[1]) if row[1] else 0
        cost_cents = row[2] if row[2] else 0
        cost_usd = cost_cents / 100.0 if cost_cents else 0
        formatted_rows.append([str(day), str(tokens), f"{cost_usd:.2f}"])

    table = _format_table(headers, formatted_rows)

    report = f"DAILY REPORT (Last 7 Days)\n\n{table}"
    return report


def report_overage(conn: sqlite3.Connection) -> str:
    """
    Generate a report of days and weeks that exceeded limits.

    Identifies days where token or cost limits were exceeded,
    and weeks where weekly limits were exceeded.

    Args:
        conn: sqlite3.Connection

    Returns:
        A formatted string report ready to print
    """
    cursor = conn.cursor()

    # Get all unique dates with data
    cursor.execute(
        """
    SELECT DISTINCT DATE(bucket_start) FROM usage_records
    UNION
    SELECT DISTINCT DATE(bucket_start) FROM cost_records
    ORDER BY 1 DESC
    """
    )

    dates = [row[0] for row in cursor.fetchall()]

    overage_data = []

    # Check each date against limits
    for date in dates:
        cursor.execute(
            """
        SELECT daily_token_limit, daily_cost_limit_cents
        FROM plan_limits
        WHERE effective_date <= ?
        ORDER BY effective_date DESC
        LIMIT 1
        """,
            (date,),
        )
        limit_row = cursor.fetchone()

        if limit_row:
            daily_token_limit, daily_cost_limit_cents = limit_row

            # Get daily token totals
            cursor.execute(
                """
            SELECT COALESCE(SUM(uncached_input_tokens + cache_read_input_tokens + output_tokens), 0)
            FROM usage_records
            WHERE DATE(bucket_start) = ?
            """,
                (date,),
            )
            token_row = cursor.fetchone()
            tokens = token_row[0] if token_row else 0

            # Get daily cost totals
            cursor.execute(
                """
            SELECT COALESCE(SUM(amount_cents), 0)
            FROM cost_records
            WHERE DATE(bucket_start) = ?
            """,
                (date,),
            )
            cost_row = cursor.fetchone()
            costs = cost_row[0] if cost_row else 0

            # Check for overage
            if (daily_token_limit and tokens > daily_token_limit) or (
                daily_cost_limit_cents and costs > daily_cost_limit_cents
            ):
                overage_type = []
                if daily_token_limit and tokens > daily_token_limit:
                    overage_type.append(f"Tokens: {tokens} > {daily_token_limit}")
                if daily_cost_limit_cents and costs > daily_cost_limit_cents:
                    overage_type.append(f"Cost: ${costs/100:.2f} > ${daily_cost_limit_cents/100:.2f}")

                if overage_type:
                    overage_data.append([date, ", ".join(overage_type)])

    # Format as table
    headers = ["Date", "Overage Details"]
    table = _format_table(headers, overage_data)

    if overage_data:
        report = f"OVERAGE REPORT\n\n{table}"
    else:
        report = "OVERAGE REPORT\n\nNo overages detected."

    return report
