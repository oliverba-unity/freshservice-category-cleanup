class FreshserviceError(Exception):
    """Base exception for Freshservice API."""
    pass

class FreshserviceHTTPError(FreshserviceError):
    """Raised for 4xx or 5xx responses (excluding 429)."""
    def __init__(self, message, response):
        super().__init__(message)
        self.response = response

class FreshserviceRateLimitError(FreshserviceError):
    """Raised when max retries are exceeded for 429 Rate Limiting."""
    pass