from __future__ import annotations
from datetime import datetime
from typing import Any, TYPE_CHECKING

# Prevent "Circular Import" error
if TYPE_CHECKING:
    from freshservice_api.freshservice_api import FreshserviceApi

type TicketPayload = list[list[str | int | bool | list[str] | dict[str, Any]]]

class Ticket:
    def __init__(self, client: FreshserviceApi, ticket_id: int | None = None, **kwargs):
        if "id" in kwargs:
            raise ValueError("'id' must be passed as an argument, not in keyword arguments.")

        self.client = client
        self.id = ticket_id

        # Attributes
        self.subject: str | None = kwargs.get("subject")
        self.description: str | None = kwargs.get("description")
        self.description_text: str | None = kwargs.get("description_text")
        self.status: int | None = kwargs.get("status")
        self.priority: int | None = kwargs.get("priority")
        self.type: str | None = kwargs.get("type")
        self.source: int | None = kwargs.get("source")
        self.category: str | None = kwargs.get("category")
        self.sub_category: str | None = kwargs.get("sub_category")
        self.item_category: str | None = kwargs.get("item_category")

        # Dates
        self.created_at: datetime | None = self._parse_date(kwargs.get("created_at"))
        self.updated_at: datetime | None = self._parse_date(kwargs.get("updated_at"))
        self.due_by: datetime | None = self._parse_date(kwargs.get("due_by"))
        self.fr_due_by: datetime | None = self._parse_date(kwargs.get("fr_due_by"))

        # Associations
        self.group_id: int | None = kwargs.get("group_id")
        self.department_id: int | None = kwargs.get("department_id")
        self.requester_id: int | None = kwargs.get("requester_id")
        self.requested_for_id: int | None = kwargs.get("requested_for_id")
        self.responder_id: int | None = kwargs.get("responder_id")
        self.workspace_id: int | None = kwargs.get("workspace_id")
        self.sla_policy_id: int | None = kwargs.get("sla_policy_id")
        self.applied_business_hours: int | None = kwargs.get("applied_business_hours")

        # State
        self.fr_escalated: bool | None = kwargs.get("fr_escalated")
        self.is_escalated: bool | None = kwargs.get("is_escalated")
        self.deleted: bool | None = kwargs.get("deleted")
        self.spam: bool | None = kwargs.get("spam")
        self.created_within_business_hours: bool | None = kwargs.get("created_within_business_hours")

        # Collections
        self.fwd_emails: list[str] = kwargs.get("fwd_emails", [])
        self.reply_cc_emails: list[str] = kwargs.get("reply_cc_emails", [])
        self.cc_emails: list[str] = kwargs.get("cc_emails", [])
        self.to_emails: list[str] | None = kwargs.get("to_emails")
        self.bcc_emails: list[str] | None = kwargs.get("bcc_emails")
        self.attachments: list[dict[str, Any]] = kwargs.get("attachments", [])
        self.custom_fields: dict[str, Any] = kwargs.get("custom_fields", {})

        self.email_config_id: int | None = kwargs.get("email_config_id")
        self.tasks_dependency_type: int | None = kwargs.get("tasks_dependency_type")
        self.resolution_notes: str | None = kwargs.get("resolution_notes")
        self.resolution_notes_html: str | None = kwargs.get("resolution_notes_html")

    @property
    def path(self) -> str:
        return f"tickets/{self.id}" if self.id else "tickets"

    def create(self, payload: TicketPayload | None = None) -> Ticket:
        if self.id:
            raise ValueError("Cannot specify ID when creating ticket.")
        body = dict(payload) if payload is not None else self._to_payload()
        return self._hydrate(self.client._request("POST", self.path, json=body))

    def get(self) -> Ticket:
        if not self.id:
            raise ValueError("Cannot get a ticket without an ID.")
        return self._hydrate(self.client._request("GET", self.path))

    def update(self, payload: TicketPayload | None = None) -> Ticket:
        if not self.id:
            raise ValueError("Cannot update an unsaved ticket.")
        body = dict(payload) if payload is not None else self._to_payload()
        return self._hydrate(self.client._request("PUT", self.path, json=body))

    def delete(self) -> dict[str, Any]:
        if not self.id:
            raise ValueError("Cannot delete a ticket without an ID.")
        response = self.client._request("DELETE", self.path)
        self.deleted = True
        return response

    def to_dict(self) -> dict[str, Any]:
        data = {k: v for k, v in self.__dict__.items() if not k.startswith('_') and k != 'client'}
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = self._format_date(v)
        return data

    def __repr__(self) -> str:
        state = f"#{self.id}" if self.id else "NEW"
        return f"<Ticket {state}: {self.subject}>"

    def _to_payload(self) -> dict[str, Any]:
        exclude = (
            'applied_business_hours',
            'attachments',
            'client',
            'created_at',
            'created_within_business_hours',
            'deleted',
            'description_text',
            'fr_escalated',
            'fwd_emails',
            'id',
            'is_escalated',
            'reply_cc_emails',
            'sla_policy_id',
            'spam',
            'tasks_dependency_type',
            'updated_at',
        )
        payload = {}
        for k, v in self.__dict__.items():
            if k in exclude or k.startswith('_') or v is None:
                continue
            payload[k] = self._format_date(v) if isinstance(v, datetime) else v
        return payload

    def _hydrate(self, data: dict[str, Any]) -> Ticket:
        data = data.get("ticket", data) if isinstance(data, dict) else data
        date_fields = ('created_at', 'updated_at', 'due_by', 'fr_due_by')
        if isinstance(data, dict):
            for key, value in data.items():
                if hasattr(self, key):
                    val = self._parse_date(value) if key in date_fields else value
                    setattr(self, key, val)
        return self

    def _parse_date(self, date_str: Any) -> datetime | None:
        if not isinstance(date_str, str): return None
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))

    def _format_date(self, dt: datetime | None) -> str | None:
        if not dt: return None
        return dt.isoformat().replace("+00:00", "Z")