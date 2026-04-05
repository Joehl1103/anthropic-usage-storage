"""
Tests for the Anthropic Admin API client module.

These tests validate the API module's functions for building requests,
handling pagination, and fetching usage and cost data from Anthropic's
Admin API. Uses mocking to avoid making real API calls.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import httpx

from src.api import (
    _build_headers,
    _build_usage_params,
    _build_cost_params,
    _fetch_paginated,
    fetch_usage,
    fetch_costs,
    compute_time_window,
    BASE_URL,
    USAGE_ENDPOINT,
    COST_ENDPOINT,
)


class TestBuildHeaders:
    """Test the _build_headers function."""

    def test_build_headers(self):
        """Verify headers dict contains API key, Anthropic version, and content type."""
        api_key = "test-key-123"
        headers = _build_headers(api_key)

        assert headers["x-api-key"] == api_key
        assert headers["anthropic-version"] == "2023-06-01"
        assert headers["content-type"] == "application/json"


class TestBuildUsageParams:
    """Test the _build_usage_params function."""

    def test_build_usage_params_no_page(self):
        """Verify usage params without page token."""
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"
        bucket_width = "1h"

        params = _build_usage_params(start, end, bucket_width, None)

        assert params["start_date"] == start
        assert params["end_date"] == end
        assert params["bucket_width"] == bucket_width
        assert "model" in params["group_by"]
        assert "workspace_id" in params["group_by"]
        assert "api_key_id" in params["group_by"]
        assert "service_tier" in params["group_by"]
        assert "page" not in params

    def test_build_usage_params_with_page(self):
        """Verify page token is included in params when provided."""
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"
        bucket_width = "1h"
        page = "page_token_123"

        params = _build_usage_params(start, end, bucket_width, None, page)

        assert params["page"] == page
        assert params["start_date"] == start

    def test_build_usage_params_custom_group_by(self):
        """Verify custom group_by values override defaults."""
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"
        custom_group_by = ["model", "api_key_id"]

        params = _build_usage_params(start, end, "1h", custom_group_by)

        assert params["group_by"] == custom_group_by


class TestBuildCostParams:
    """Test the _build_cost_params function."""

    def test_build_cost_params(self):
        """Verify cost params have correct structure and default group_by."""
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"
        bucket_width = "1h"

        params = _build_cost_params(start, end, bucket_width, None)

        assert params["start_date"] == start
        assert params["end_date"] == end
        assert params["bucket_width"] == bucket_width
        assert "model" in params["group_by"]
        assert "workspace_id" in params["group_by"]
        assert "description" in params["group_by"]
        assert "page" not in params

    def test_build_cost_params_with_page(self):
        """Verify page token is included in cost params."""
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"
        page = "page_token_456"

        params = _build_cost_params(start, end, "1h", None, page)

        assert params["page"] == page


class TestFetchPaginated:
    """Test the _fetch_paginated function."""

    @patch("src.api.httpx.Client")
    def test_fetch_paginated_single_page(self, mock_client_class):
        """Verify single page response is returned correctly."""
        # Mock the client and its response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": "1", "value": 100}, {"id": "2", "value": 200}],
            "has_more": False,
        }

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client

        url = "https://example.com/api"
        headers = {"x-api-key": "test"}
        params = {"start_date": "2024-01-01T00:00:00Z"}

        result = _fetch_paginated(url, headers, params)

        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"
        # Verify httpx.Client was instantiated with 30s timeout
        mock_client_class.assert_called_once_with(timeout=30)
        # Verify get was called with correct URL and params
        mock_client.get.assert_called_once_with(url, headers=headers, params=params)

    @patch("src.api.httpx.Client")
    def test_fetch_paginated_multiple_pages(self, mock_client_class):
        """Verify pagination collects data from multiple pages."""
        # Mock responses for two pages
        mock_response1 = MagicMock()
        mock_response1.json.return_value = {
            "data": [{"id": "1", "value": 100}],
            "has_more": True,
            "next_page": "page_token_xyz",
        }

        mock_response2 = MagicMock()
        mock_response2.json.return_value = {
            "data": [{"id": "2", "value": 200}],
            "has_more": False,
        }

        mock_client = MagicMock()
        mock_client.get.side_effect = [mock_response1, mock_response2]
        mock_client_class.return_value.__enter__.return_value = mock_client

        url = "https://example.com/api"
        headers = {"x-api-key": "test"}
        params = {"start_date": "2024-01-01T00:00:00Z"}

        result = _fetch_paginated(url, headers, params)

        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"
        # Verify two GET calls were made
        assert mock_client.get.call_count == 2

    @patch("src.api.httpx.Client")
    def test_fetch_paginated_http_error(self, mock_client_class):
        """Verify HTTP errors are raised appropriately."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized", request=MagicMock(), response=MagicMock(status_code=401)
        )
        mock_client_class.return_value.__enter__.return_value = mock_client

        url = "https://example.com/api"
        headers = {"x-api-key": "invalid"}
        params = {"start_date": "2024-01-01T00:00:00Z"}

        with pytest.raises(httpx.HTTPStatusError):
            _fetch_paginated(url, headers, params)


class TestFetchUsage:
    """Test the fetch_usage function."""

    @patch("src.api._fetch_paginated")
    def test_fetch_usage_calls_correct_endpoint(self, mock_fetch_paginated):
        """Verify fetch_usage calls the correct endpoint with proper params."""
        mock_fetch_paginated.return_value = [{"model": "claude-3", "usage": 1000}]

        api_key = "test-key"
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"

        result = fetch_usage(api_key, start, end)

        assert result == [{"model": "claude-3", "usage": 1000}]

        # Verify _fetch_paginated was called with the usage endpoint
        mock_fetch_paginated.assert_called_once()
        call_args = mock_fetch_paginated.call_args
        assert call_args[0][0] == USAGE_ENDPOINT  # URL is the first positional arg
        assert call_args[0][1]["x-api-key"] == api_key  # Headers dict
        assert call_args[0][2]["start_date"] == start  # Params dict

    @patch("src.api._fetch_paginated")
    def test_fetch_usage_custom_bucket_width(self, mock_fetch_paginated):
        """Verify custom bucket_width is passed through."""
        mock_fetch_paginated.return_value = []

        fetch_usage("key", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", bucket_width="1d")

        call_args = mock_fetch_paginated.call_args
        params = call_args[0][2]
        assert params["bucket_width"] == "1d"

    @patch("src.api._fetch_paginated")
    def test_fetch_usage_custom_group_by(self, mock_fetch_paginated):
        """Verify custom group_by is passed through."""
        mock_fetch_paginated.return_value = []
        custom_group = ["model"]

        fetch_usage("key", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", group_by=custom_group)

        call_args = mock_fetch_paginated.call_args
        params = call_args[0][2]
        assert params["group_by"] == custom_group


class TestFetchCosts:
    """Test the fetch_costs function."""

    @patch("src.api._fetch_paginated")
    def test_fetch_costs_calls_correct_endpoint(self, mock_fetch_paginated):
        """Verify fetch_costs calls the correct endpoint with proper params."""
        mock_fetch_paginated.return_value = [{"model": "claude-3", "cost": 50}]

        api_key = "test-key"
        start = "2024-01-01T00:00:00Z"
        end = "2024-01-02T00:00:00Z"

        result = fetch_costs(api_key, start, end)

        assert result == [{"model": "claude-3", "cost": 50}]

        # Verify _fetch_paginated was called with the cost endpoint
        mock_fetch_paginated.assert_called_once()
        call_args = mock_fetch_paginated.call_args
        assert call_args[0][0] == COST_ENDPOINT  # URL is the first positional arg
        assert call_args[0][1]["x-api-key"] == api_key  # Headers dict
        assert call_args[0][2]["start_date"] == start  # Params dict

    @patch("src.api._fetch_paginated")
    def test_fetch_costs_custom_group_by(self, mock_fetch_paginated):
        """Verify custom group_by is passed through."""
        mock_fetch_paginated.return_value = []
        custom_group = ["model", "workspace_id"]

        fetch_costs("key", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", group_by=custom_group)

        call_args = mock_fetch_paginated.call_args
        params = call_args[0][2]
        assert params["group_by"] == custom_group


class TestComputeTimeWindow:
    """Test the compute_time_window function."""

    def test_compute_time_window(self):
        """Verify time window has correct format and difference."""
        lookback_hours = 24

        start, end = compute_time_window(lookback_hours)

        # Verify both are strings in RFC 3339 format (contain Z at end)
        assert isinstance(start, str)
        assert isinstance(end, str)
        assert start.endswith("Z")
        assert end.endswith("Z")

        # Parse back to datetime for validation
        # Remove Z and parse
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))

        # Verify the difference is approximately lookback_hours
        diff = end_dt - start_dt
        assert abs(diff.total_seconds() - (lookback_hours * 3600)) < 60  # Allow 1 min tolerance

        # Verify start is before end
        assert start_dt < end_dt

    def test_compute_time_window_hour_truncation(self):
        """Verify end time is truncated to the hour."""
        start, end = compute_time_window(1)

        # Parse end datetime
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))

        # Minutes and seconds should be 0
        assert end_dt.minute == 0
        assert end_dt.second == 0

    def test_compute_time_window_various_lookbacks(self):
        """Verify time window works with various lookback values."""
        for lookback in [1, 6, 24, 168]:  # 1h, 6h, 1d, 1w
            start, end = compute_time_window(lookback)
            start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))

            diff_hours = (end_dt - start_dt).total_seconds() / 3600
            assert abs(diff_hours - lookback) < 0.02  # Allow 1 minute tolerance
