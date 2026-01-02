import httpx
import time
from typing import Any
from .ticket import Ticket
from .rate_limit_controller import RateLimitController
from .exceptions import FreshserviceHTTPError, FreshserviceRateLimitError

class FreshserviceApi:
    """
    Freshservice API Client.
    """

    def __init__(self, api_key: str, domain: str, headroom: int = 10):
        self.api_key = api_key
        self.api_version = "v2"
        self.base_url = f"https://{domain}/api/{self.api_version}"

        self.client = httpx.Client(
            auth=(api_key, "X"),
            http2=True,
            headers={"Content-Type": "application/json"},
            timeout=60.0
        )

        self.controller = RateLimitController(headroom=headroom)

    def _request(self, method: str, path: str, max_retries: int = 5, **kwargs) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        attempts = 0

        while attempts <= max_retries:

            self.controller.block_until_ready()

            try:
                response = self.client.request(method, url, **kwargs)
                self.controller.update_and_notify(response.headers)

                if response.status_code == 429:
                    attempts += 1
                    if attempts > max_retries:
                        print(f"❌ Max retries ({max_retries}) hit for 429.")
                        break

                    retry_after = int(response.headers.get("Retry-After", 5))
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return {} if response.status_code == 204 else response.json()

            except httpx.RequestError as e:
                self.controller.update_and_notify({}) # Release slot with empty headers
                raise FreshserviceHTTPError(f"Request Error: {e}", response=None) from None

        raise FreshserviceRateLimitError(f"All {max_retries} retries have failed - aborting.")

    def ticket(self) -> Ticket:
        return Ticket(self)

    def close(self):
        """Close the HTTP connection."""
        self.client.close()