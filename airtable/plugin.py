import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from lutraai.augmented_request_client import AugmentedTransport


def _resolve_error_message(status_code: int, text: str) -> str:
    """
    Returns a more human-readable error message from semi-structured error responses.

    See examples here:
    https://airtable.com/developers/web/api/errors#example-error-responses
    """
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return text
    error = data.get("error")
    match (error := data.get("error")):
        case str():
            return (
                f"{status_code} {httpx.codes.get_reason_phrase(status_code)}: {error}"
            )
        case dict():
            error_type = error.get("type")
            error_message = error.get("message")
            if not isinstance(error_type, str) or not isinstance(error_message, str):
                return text
            return (
                f"{status_code} {httpx.codes.get_reason_phrase(status_code)}: "
                f"{error_type}: {error_message}"
            )
        case _:
            return text


@dataclass
class AirtableRecord:
    id: str
    created_time: datetime
    fields: dict[str, Any]


def airtable_record_list(baseId: str, tableIdOrName: str) -> list[AirtableRecord]:
    """
    Return results of an Airtable `list records` API call.
    baseId must be an ID and not a name.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        data = (
            client.get(f"https://api.airtable.com/v0/{baseId}/{tableIdOrName}")
            .raise_for_status()
            .json()
        )
    return [
        AirtableRecord(
            id=record["id"],
            created_time=datetime.fromisoformat(record["createdTime"]),
            fields=record["fields"],
        )
        for record in data["records"]
    ]


def airtable_record_create(
    baseId: str, tableIdOrName: str, fields: dict[str, Any]
) -> AirtableRecord:
    """
    Create a record using the Airtable `create records` API call with a POST.
    baseId must be an ID and not a name.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.post(
            f"https://api.airtable.com/v0/{baseId}/{tableIdOrName}",
            json={"fields": fields},
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message(response.status_code, response.text)
            )
        data = response.json()
    return AirtableRecord(
        id=data["id"],
        created_time=datetime.fromisoformat(data["createdTime"]),
        fields=data["fields"],
    )


def airtable_record_update_patch(
    baseId: str, tableIdOrName: str, recordId: str, fields: dict[str, Any]
) -> None:
    """
    Update a record using the Airtable `update record` API call with a PATCH.
    baseId must be an ID and not a name.
    recordId must be an ID and not a name.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.patch(
            f"https://api.airtable.com/v0/{baseId}/{tableIdOrName}/{recordId}",
            json={"fields": fields},
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message(response.status_code, response.text)
            )
