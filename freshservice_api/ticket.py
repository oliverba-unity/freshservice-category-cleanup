from __future__ import annotations
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .freshservice_api import FreshserviceApi

class Ticket:
    """
    Service wrapper for Freshservice Tickets.
    """

    def __init__(self, client: FreshserviceApi):
        self.client = client

    def _path(self, ticket_id: int | None = None) -> str:
        """Internal helper to construct the URL path."""
        return f"tickets/{ticket_id}" if ticket_id else "tickets"

    def get(self, ticket_id: int) -> dict[str, Any]:
        """Fetches a specific ticket by ID."""
        response = self.client._request("GET", self._path(ticket_id))
        return response["ticket"]

    def create(self, data: dict[str, Any]) -> dict[str, Any]:
        """Creates a new ticket."""
        payload = {"ticket": data}
        response = self.client._request("POST", self._path(), json=payload)
        return response["ticket"]

    def update(self, ticket_id: int, data: dict[str, Any]) -> dict[str, Any]:
        """Updates an existing ticket by ID with the provided dictionary."""
        payload = {"ticket": data}
        response = self.client._request("PUT", self._path(ticket_id), json=payload)
        return response["ticket"]

    def delete(self, ticket_id: int) -> dict[str, Any]:
        """Deletes a specific ticket by ID."""
        return self.client._request("DELETE", self._path(ticket_id))
