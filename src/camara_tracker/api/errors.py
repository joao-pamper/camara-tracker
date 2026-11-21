"""Exceptions raised by the Câmara API client."""


class ApiError(Exception):
    """Raised when an API request fails."""

    def __init__(
        self,
        status_code: int | None,
        url: str,
        message: str = "API request failed",
    ) -> None:
        self.status_code: int | None = status_code
        self.url: str = url
        super().__init__(f"{message} (status={status_code}, url={url})")


class RetryExhaustedError(ApiError):
    """Raised when a retryable API request exhausts its retries."""
