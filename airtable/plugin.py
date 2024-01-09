from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class AirtableRecord:
    id: str
    created_time: datetime
    fields: dict[str, Any]


def airtable_record_list(baseId: str, tableIdOrName: str) -> list[AirtableRecord]:
    """
    Return results of an Airtable `list records` API call.
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
        data = (
            client.post(
                f"https://api.airtable.com/v0/{baseId}/{tableIdOrName}",
                json={"fields": fields},
            )
            .raise_for_status()
            .json()
        )
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
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        client.patch(
            f"https://api.airtable.com/v0/{baseId}/{tableIdOrName}/{recordId}",
            json={"fields": fields},
        ).raise_for_status()
