"""Tests for API exceptions."""

from camara_tracker.api import ApiError, RetryExhaustedError


def test_api_error_exposes_status_and_url() -> None:
    """ApiError keeps the failing status and URL."""
    error = ApiError(404, "https://example.test/deputados", "not found")

    assert error.status_code == 404
    assert error.url == "https://example.test/deputados"
    assert "not found" in str(error)


def test_retry_exhausted_is_an_api_error() -> None:
    """RetryExhaustedError is a specialized ApiError."""
    error = RetryExhaustedError(503, "https://example.test/deputados")

    assert isinstance(error, ApiError)
    assert error.status_code == 503
