from __future__ import annotations

from typing import Any

import requests

from freshservice_api.ticket import Ticket


class FreshserviceApi:
    """Main interface for Freshservice. Handles Auth and Transport."""

    def __init__(self, api_key: str, domain: str):
        self.api_key = api_key
        self.domain = domain.removeprefix("https://").removeprefix("http://").removesuffix("/")
        self.base_url = f"https://{self.domain}"

        self.session = requests.Session()
        self.session.auth = (api_key, "X")
        self.session.headers.update({"Content-Type": "application/json"})

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        """
        Centralized request handler.
        Raises requests.exceptions.HTTPError for non-200/300 responses.
        """
        url = f"{self.base_url}/api/v2/{path.lstrip('/')}"

        with self.session.request(method, url, **kwargs) as response:
            try:
                # This throws an HTTPError for 4xx or 5xx responses
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                # Freshservice often provides JSON error details.
                # We try to append them to the exception for better debugging.
                try:
                    error_data = response.json()
                    custom_msg = f"{e} | API Details: {error_data}"
                    raise requests.exceptions.HTTPError(custom_msg, response=response) from None
                except (ValueError, KeyError):
                    # If it's not JSON, just raise the original error
                    raise e

            # Handle 204 No Content (Standard for DELETE)
            if response.status_code == 204:
                return {}

            data = response.json()
            # Flatten the response if it's wrapped in a "ticket" key
            return data.get("ticket", data) if isinstance(data, dict) else data

    def ticket(self, ticket_id: int | None = None, **kwargs) -> Ticket:
        """Returns a Ticket instance scoped to this ApiClient."""
        return Ticket(self, ticket_id, **kwargs)
