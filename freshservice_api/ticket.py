from __future__ import annotations
from httpx import Response
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .freshservice_api import FreshserviceApi

def _path(ticket_id: int | None = None) -> str:
    return f"tickets/{ticket_id}" if ticket_id else "tickets"

class Ticket:
    """
    Service wrapper for Freshservice Tickets.
    """

    def __init__(self, client: FreshserviceApi):
        self.client = client

    def get(self, ticket_id: int) -> Response:
        """Fetches a specific ticket by ID."""
        response = self.client.request("GET", _path(ticket_id))
        return response

    def create(self, data: dict[str, Any]) -> Response:
        """Creates a new ticket."""
        payload = {"ticket": data}
        response = self.client.request("POST", _path(), json=payload)
        return response

    def update(self, ticket_id: int, data: dict[str, Any]) -> Response:
        """Updates an existing ticket by ID with the provided dictionary."""
        payload = {"ticket": data}
        response = self.client.request("PUT", _path(ticket_id), json=payload)
        return response

    def delete(self, ticket_id: int) -> Response:
        """Deletes a specific ticket by ID."""
        response = self.client.request("DELETE", _path(ticket_id))
        return response
