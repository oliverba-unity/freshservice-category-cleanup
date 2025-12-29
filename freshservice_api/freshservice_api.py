import time
import requests
from typing import Any
from .ticket import Ticket
from .exceptions import FreshserviceHTTPError, FreshserviceRateLimitError

class FreshserviceApi:
    def __init__(self, api_key: str, domain: str):
        self.api_key = api_key
        self.api_version = "v2"
        self.base_url = f"https://{domain}/api/{self.api_version}"

        self.session = requests.Session()
        self.session.auth = (api_key, "X")
        self.session.headers.update({"Content-Type": "application/json"})

        self.rate_limit_total: int | None = None
        self.rate_limit_remaining: int | None = None

    def _request(self, method: str, path: str, max_retries: int = 5, **kwargs) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        attempts = 0

        while attempts <= max_retries:
            response: requests.Response = self.session.request(method, url, **kwargs)

            self.rate_limit_total = int(response.headers.get("x-ratelimit-total", 0))
            self.rate_limit_remaining = int(response.headers.get("x-ratelimit-remaining", 0))

            if response.status_code == 429:
                attempts += 1
                if attempts > max_retries:
                    break

                # Priority 1: Retry-After | Priority 2: Exponential Backoff
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after else (2 ** attempts)

                time.sleep(wait_time)
                continue

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                try:
                    error_data = response.json()
                except ValueError:
                    error_data = response.text

                # Raise our custom HTTP error
                raise FreshserviceHTTPError(f"{e} | {error_data}", response=response) from None

            return {} if response.status_code == 204 else response.json()

        raise FreshserviceRateLimitError(f"Max retries ({max_retries}) reached for 429 errors.")

    def ticket(self) -> Ticket:
        return Ticket(self)