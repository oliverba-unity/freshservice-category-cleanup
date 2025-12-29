import time
from datetime import datetime, timedelta
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
        self.rate_limit_hit: bool = False
        self.retry_after_until: datetime | None = None

    def _request(self, method: str, path: str, max_retries: int = 5, **kwargs) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        attempts = 0

        while attempts <= max_retries:
            # If we know we are currently rate limited, wait before even trying
            if self.rate_limit_hit and self.retry_after_until:
                now = datetime.now()
                if now < self.retry_after_until:
                    wait_seconds = (self.retry_after_until - now).total_seconds()
                    print(
                        f"Currently rate limited. Waiting {wait_seconds:.0f}s...")
                    time.sleep(wait_seconds)

                # Reset status after waiting
                self.rate_limit_hit = False
                self.retry_after_until = None

            response: requests.Response = self.session.request(method, url, **kwargs)

            self.rate_limit_total = int(response.headers.get("x-ratelimit-total", 0)) or self.rate_limit_total
            self.rate_limit_remaining = int(
                response.headers.get("x-ratelimit-remaining", 0)) or self.rate_limit_remaining

            if response.status_code == 429:
                attempts += 1
                if attempts > max_retries:
                    break

                self.rate_limit_hit = True
                retry_header = response.headers.get("Retry-After")
                wait_time = int(retry_header) if retry_header else (2 ** attempts)

                # Set absolute datetime for when it's safe to retry
                self.retry_after_until = datetime.now() + timedelta(seconds=wait_time)

                print(f"Rate limit 429 received. Blocked until {self.retry_after_until.strftime('%H:%M:%S')}")
                continue

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                try:
                    error_data = response.json()
                except ValueError:
                    error_data = response.text
                raise FreshserviceHTTPError(f"{e} | {error_data}", response=response) from None

            return {} if response.status_code == 204 else response.json()

        raise FreshserviceRateLimitError(f"Max retries ({max_retries}) reached for 429 errors.")

    def ticket(self) -> Ticket:
        return Ticket(self)