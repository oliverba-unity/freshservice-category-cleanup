from __future__ import annotations
from typing import Any
import requests

from freshservice_api.ticket import Ticket

class FreshserviceApi:
    """Interface for Freshservice API."""

    def __init__(self, api_key: str, domain: str):
        self.api_key = api_key
        self.api_version = "v2"
        self.base_url = f"https://{domain}/api/{self.api_version}"

        self.session = requests.Session()
        self.session.auth = (api_key, "X")
        self.session.headers.update({"Content-Type": "application/json"})

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        """Make a request to the Freshservice API."""
        url = f"{self.base_url}/{path.lstrip('/')}"

        with self.session.request(method, url, **kwargs) as response:
            try:
                # Throw HTTPError for 4xx or 5xx responses
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                # Try to append response JSON as it may contain error details
                try:
                    error_response = response.json()
                    custom_msg = f"{e} | Response body: {error_response}"
                    raise requests.exceptions.HTTPError(custom_msg, response=response) from None
                except (ValueError, KeyError):
                    # If the response is not JSON, just raise the original error
                    raise e

            if response.status_code == 204:
                return {}

            return response.json()

    def ticket(self) -> Ticket:
        """Returns the stateless Ticket API wrapper."""
        return Ticket(self)
