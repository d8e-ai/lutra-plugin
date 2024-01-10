import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from lutraai.augmented_request_client import AugmentedTransport


def _resolve_error_message_no_schema(status_code: int, text: str) -> tuple[str, bool]:
    """
    Returns a more human-readable error message from semi-structured error responses.

    See examples here:
    https://airtable.com/developers/web/api/errors#example-error-responses
    """
    include_schema = status_code == httpx.codes.UNPROCESSABLE_ENTITY
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return text, include_schema
    match (error := data.get("error")):
        case str():
            return (
                f"{status_code} {httpx.codes.get_reason_phrase(status_code)}: {error}",
                include_schema,
            )
        case dict():
            error_type = error.get("type")
            error_message = error.get("message")
            if not isinstance(error_type, str) or not isinstance(error_message, str):
                return text, include_schema
            return (
                (
                    f"{status_code} {httpx.codes.get_reason_phrase(status_code)}: "
                    f"{error_type}: {error_message}"
                ),
                include_schema,
            )
        case _:
            return text, include_schema


def _resolve_error_message(
    client: httpx.Client,
    base_id: str,
    table_id_or_name: str,
    status_code: int,
    text: str,
) -> str:
    msg, include_schema = _resolve_error_message_no_schema(status_code, text)
    if include_schema:
        try:
            data = (
                client.get(f"https://api.airtable.com/v0/meta/bases/{base_id}/tables")
                .raise_for_status()
                .json()
            )
            table_id_names = []
            for table in data["tables"]:
                table_id_names.append(f"{table['id']}({table['name']})")
            schema = None
            for table in data["tables"]:
                if table["id"] == table_id_or_name:
                    schema = table["fields"]
            if not schema:
                for table in data["tables"]:
                    if table["name"] == table_id_or_name:
                        schema = table["fields"]
            if not schema:
                return (
                    f"{msg}; {table_id_or_name} not found in "
                    f"tables: {sorted(table_id_names)}"
                )
            return f"{msg}; schema of table `{table_id_or_name}`: {json.dumps(schema)}"
        except Exception as e:
            return f"{msg}; (error fetching schema of {table_id_or_name}: {e})"
    return msg


@dataclass
class AirtableRecord:
    id: str
    created_time: datetime
    fields: dict[str, Any]


def airtable_record_list(base_id: str, table_id_or_name: str) -> list[AirtableRecord]:
    """
    Return results of an Airtable `list records` API call.
    base_id must be an ID and not a name.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.get(
            f"https://api.airtable.com/v0/{base_id}/{table_id_or_name}"
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message_no_schema(response.status_code, response.text)
            )
        data = response.json()
    return [
        AirtableRecord(
            id=record["id"],
            created_time=datetime.fromisoformat(record["createdTime"]),
            fields=record["fields"],
        )
        for record in data["records"]
    ]


def airtable_record_create(
    base_id: str, table_id_or_name: str, fields: dict[str, Any]
) -> AirtableRecord:
    """
    Create a record using the Airtable `create records` API call with a POST.
    base_id must be an ID and not a name.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.post(
            f"https://api.airtable.com/v0/{base_id}/{table_id_or_name}",
            json={"fields": fields},
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message(
                    client,
                    base_id,
                    table_id_or_name,
                    response.status_code,
                    response.text,
                )
            )
        data = response.json()
    return AirtableRecord(
        id=data["id"],
        created_time=datetime.fromisoformat(data["createdTime"]),
        fields=data["fields"],
    )


def airtable_record_update_patch(
    base_id: str, table_id_or_name: str, record_id: str, fields: dict[str, Any]
) -> None:
    """
    Update a record using the Airtable `update record` API call with a PATCH.
    base_id must be an ID and not a name.
    record_id must be an ID and not a name.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.patch(
            f"https://api.airtable.com/v0/{base_id}/{table_id_or_name}/{record_id}",
            json={"fields": fields},
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message(
                    client,
                    base_id,
                    table_id_or_name,
                    response.status_code,
                    response.text,
                )
            )
