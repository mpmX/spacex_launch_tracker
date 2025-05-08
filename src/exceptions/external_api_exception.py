from typing import Optional


class ExternalAPIException(Exception):
    """Custom exception for errors encountered while interacting with an external API."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        url: Optional[str] = None,
        response_text: Optional[str] = None,
        original_exception: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.url = url
        self.response_text = response_text
        self.original_exception = original_exception

    def __str__(self):
        details = [f"Message: {self.message}"]
        if self.status_code is not None:
            details.append(f"Status Code: {self.status_code}")
        if self.url:
            details.append(f"URL: {self.url}")
        if self.response_text:
            display_text = (
                self.response_text[:200] + "..."
                if len(self.response_text) > 200
                else self.response_text
            )
            details.append(f"Response Text: {display_text}")
        if self.original_exception:
            details.append(
                f"Original Exception: {type(self.original_exception).__name__}: {self.original_exception}"
            )
        return f"ExternalAPIException: {'; '.join(details)}"
