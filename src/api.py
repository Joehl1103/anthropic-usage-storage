"""
Anthropic Admin API client for fetching usage and cost statistics.

This module provides a clean interface to the Anthropic Admin API,
handling pagination, request building, and time window computation.
All API calls are made over HTTPS using httpx for robust connection handling.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx


# API endpoint constants
BASE_URL = "https://api.anthropic.com/v1/organizations"
USAGE_ENDPOINT = f"{BASE_URL}/usage_report/messages"
COST_ENDPOINT = f"{BASE_URL}/cost_report"


def _build_headers(api_key: str) -> dict:
    """
    Build HTTP headers for Anthropic Admin API requests.

    Args:
        api_key: The Anthropic API key to include in the x-api-key header.

    Returns:
        A dict with x-api-key, anthropic-version, and content-type headers.
    """
    return {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }


def _build_usage_params(
    start: str,
    end: str,
    bucket_width: str,
    group_by: Optional[list[str]] = None,
    page: Optional[str] = None,
) -> dict:
    """
    Build query parameters for the usage_report/messages endpoint.

    Args:
        start: RFC 3339 UTC datetime string marking the start of the period.
        end: RFC 3339 UTC datetime string marking the end of the period.
        bucket_width: Time bucket width (e.g., "1h", "1d").
        group_by: List of fields to group results by. Defaults to
                  ["model", "workspace_id", "api_key_id", "service_tier"].
        page: Optional pagination token from a previous response.

    Returns:
        A dict of query parameters ready to pass to httpx.
    """
    if group_by is None:
        group_by = ["model", "workspace_id", "api_key_id", "service_tier"]

    params = {
        "start_date": start,
        "end_date": end,
        "bucket_width": bucket_width,
        "group_by": group_by,
    }

    # Only include page if provided
    if page is not None:
        params["page"] = page

    return params


def _build_cost_params(
    start: str,
    end: str,
    bucket_width: str,
    group_by: Optional[list[str]] = None,
    page: Optional[str] = None,
) -> dict:
    """
    Build query parameters for the cost_report endpoint.

    Args:
        start: RFC 3339 UTC datetime string marking the start of the period.
        end: RFC 3339 UTC datetime string marking the end of the period.
        bucket_width: Time bucket width (e.g., "1h", "1d").
        group_by: List of fields to group results by. Defaults to
                  ["model", "workspace_id", "description"].
        page: Optional pagination token from a previous response.

    Returns:
        A dict of query parameters ready to pass to httpx.
    """
    if group_by is None:
        group_by = ["model", "workspace_id", "description"]

    params = {
        "start_date": start,
        "end_date": end,
        "bucket_width": bucket_width,
        "group_by": group_by,
    }

    # Only include page if provided
    if page is not None:
        params["page"] = page

    return params


def _fetch_paginated(url: str, headers: dict, params: dict) -> list[dict]:
    """
    Fetch all paginated results from an API endpoint.

    Makes GET requests to the given URL, following pagination links returned
    in the response. Continues fetching while has_more is true, collects all
    data records into a single list.

    Args:
        url: The API endpoint URL to fetch from.
        headers: HTTP headers to include in requests (must include x-api-key).
        params: Query parameters for the first request. page param is updated
                for subsequent requests as needed.

    Returns:
        A flat list of all data records from all pages.

    Raises:
        httpx.HTTPStatusError: If the API returns an error status code.
    """
    all_data = []

    with httpx.Client(timeout=30) as client:
        while True:
            # Make the GET request
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise on 4xx/5xx

            # Parse the response
            result = response.json()
            all_data.extend(result.get("data", []))

            # Check if there are more pages
            if not result.get("has_more", False):
                break

            # Prepare for next page by adding the page token
            next_page = result.get("next_page")
            if next_page:
                params["page"] = next_page

    return all_data


def fetch_usage(
    api_key: str,
    start: str,
    end: str,
    bucket_width: str = "1h",
    group_by: Optional[list[str]] = None,
) -> list[dict]:
    """
    Fetch usage statistics from the Anthropic Admin API.

    Args:
        api_key: The Anthropic API key with admin permissions.
        start: RFC 3339 UTC datetime string marking the start of the period.
        end: RFC 3339 UTC datetime string marking the end of the period.
        bucket_width: Time bucket width for aggregation. Defaults to "1h".
        group_by: List of fields to group results by. If None, uses default
                  grouping: ["model", "workspace_id", "api_key_id", "service_tier"].

    Returns:
        A list of usage records. Each record is a dict with usage metrics
        grouped by the fields specified in group_by.
    """
    headers = _build_headers(api_key)
    params = _build_usage_params(start, end, bucket_width, group_by)
    return _fetch_paginated(USAGE_ENDPOINT, headers, params)


def fetch_costs(
    api_key: str,
    start: str,
    end: str,
    bucket_width: str = "1h",
    group_by: Optional[list[str]] = None,
) -> list[dict]:
    """
    Fetch cost statistics from the Anthropic Admin API.

    Args:
        api_key: The Anthropic API key with admin permissions.
        start: RFC 3339 UTC datetime string marking the start of the period.
        end: RFC 3339 UTC datetime string marking the end of the period.
        bucket_width: Time bucket width for aggregation. Defaults to "1h".
        group_by: List of fields to group results by. If None, uses default
                  grouping: ["model", "workspace_id", "description"].

    Returns:
        A list of cost records. Each record is a dict with cost metrics
        grouped by the fields specified in group_by.
    """
    headers = _build_headers(api_key)
    params = _build_cost_params(start, end, bucket_width, group_by)
    return _fetch_paginated(COST_ENDPOINT, headers, params)


def compute_time_window(lookback_hours: int) -> tuple[str, str]:
    """
    Compute a time window relative to the current time.

    Returns an (start, end) tuple of RFC 3339 UTC datetime strings.
    The end time is truncated to the top of the current hour.
    The start time is end minus the given lookback_hours.

    Args:
        lookback_hours: Number of hours to look back from the current hour.

    Returns:
        A tuple of (start_datetime_str, end_datetime_str) in RFC 3339 format,
        both with UTC timezone.
    """
    # Get current time in UTC and truncate to the hour
    now_utc = datetime.now(timezone.utc)
    end_utc = now_utc.replace(minute=0, second=0, microsecond=0)

    # Calculate start by subtracting lookback_hours
    start_utc = end_utc - timedelta(hours=lookback_hours)

    # Format both as RFC 3339 strings
    start_str = start_utc.isoformat().replace("+00:00", "Z")
    end_str = end_utc.isoformat().replace("+00:00", "Z")

    return start_str, end_str
