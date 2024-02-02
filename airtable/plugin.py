import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlparse

import httpx
from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class AirtableBaseID:
    id: str


@dataclass
class AirtableTableID:
    """
    id can either be the table name or the table ID.
    """

    id: str


@dataclass
class AirtableRecordID:
    id: str


def airtable_parse_ids_from_url(
    url: str,
) -> Tuple[AirtableBaseID, Optional[AirtableTableID], Optional[AirtableRecordID]]:
    """
    Parse Airtable IDs from an Airtable web UI URL.  url must be in the format
    "https://airtable.com/{base_id}/{table_id}/{view_id}/{record_id}" and may include
    additional path segments or query arguments after the record ID which will be
    ignored.

    Only the base_id is required and guaranteed to be non-None.  For each other ID, if
    it is not present in the URL, the corresponding value in the tuple will be None.
    You must check that these ID values are not None before using them.
    """
    parsed_url = urlparse(url)
    if parsed_url.netloc != "airtable.com":
        raise ValueError(f"host must be airtable.com: {url}")
    match = re.search(
        r"/(?P<base_id>app[\w\d]+)(?:/(?P<table_id>tbl[\w\d]+))?(?:/(?P<view_id>viw[\w\d]+))?(?:/(?P<record_id>rec[\w\d]+))?(?:/.*)?",
        unquote(parsed_url.path),
    )
    if match:
        base_id = match.group("base_id")
        table_id = match.group("table_id")
        record_id = match.group("record_id")
        return (
            AirtableBaseID(base_id),
            AirtableTableID(table_id) if table_id else None,
            AirtableRecordID(record_id) if record_id else None,
        )
    else:
        raise ValueError(f"does not match the expected Airtable URL format: {url}")


def _resolve_error_message_no_schema(status_code: int, text: str) -> tuple[str, bool]:
    """
    Returns a more human-readable error message from semi-structured error responses.

    See examples here:
    https://airtable.com/developers/web/api/errors#example-error-responses

    The first element of the tuple is the error message.  The second element is a
    bool judgement whether the error message would benefit from schema information.
    """
    prefix = f"{status_code} {httpx.codes.get_reason_phrase(status_code)}: "
    include_schema = status_code == httpx.codes.UNPROCESSABLE_ENTITY
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return f"{prefix}{text}", include_schema
    match (error := data.get("error")):
        case str():
            return f"{prefix}{error}", include_schema
        case dict():
            error_type = error.get("type")
            error_message = error.get("message")
            if not isinstance(error_type, str) or not isinstance(error_message, str):
                return f"{prefix}{text}", include_schema
            return f"{prefix}{error_type}: {error_message}", include_schema
        case _:
            return f"{prefix}{text}", include_schema


def _resolve_error_message(
    client: httpx.Client,
    base_id: str,
    table_id_or_name: str,
    status_code: int,
    text: str,
) -> str:
    """
    Returns a more human-readable error message from semi-structured error responses.
    If the error is a 422, try to include the schema of the table in the error message.

    See examples here:
    https://airtable.com/developers/web/api/errors#example-error-responses
    """
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
    record_id: AirtableRecordID
    created_time: datetime
    fields: dict[str, Any]


def airtable_record_list(
    base_id: AirtableBaseID, table_id: AirtableTableID
) -> list[AirtableRecord]:
    """
    Return results of an Airtable `list records` API call.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.get(f"https://api.airtable.com/v0/{base_id.id}/{table_id.id}")
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message_no_schema(response.status_code, response.text)
            )
        data = response.json()
    return [
        AirtableRecord(
            record_id=AirtableRecordID(record["id"]),
            created_time=datetime.fromisoformat(record["createdTime"]),
            fields=record["fields"],
        )
        for record in data["records"]
    ]


def airtable_record_create(
    base_id: AirtableBaseID,
    table_id: AirtableTableID,
    fields: dict[str, Any],
    typecast: bool = True,
) -> AirtableRecord:
    """
    Create a record using the Airtable `create records` API call with a POST.

    If typecast is True, Airtable will try to convert the value to the appropriate cell value.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.post(
            f"https://api.airtable.com/v0/{base_id.id}/{table_id.id}",
            json={"fields": fields, "typecast": typecast},
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message(
                    client,
                    base_id.id,
                    table_id.id,
                    response.status_code,
                    response.text,
                )
            )
        data = response.json()
    return AirtableRecord(
        record_id=AirtableRecordID(data["id"]),
        created_time=datetime.fromisoformat(data["createdTime"]),
        fields=data["fields"],
    )


def airtable_record_update_patch(
    base_id: AirtableBaseID,
    table_id: AirtableTableID,
    record_id: AirtableRecordID,
    fields: dict[str, Any],
    typecast: bool = True,
) -> None:
    """
    Update a record using the Airtable `update record` API call with a PATCH and override the fields it is patching.

    If typecast is True, Airtable will try to convert the value to the appropriate cell value.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = client.patch(
            f"https://api.airtable.com/v0/{base_id.id}/{table_id.id}/{record_id.id}",
            json={"fields": fields, "typecast": typecast},
        )
        if response.status_code != httpx.codes.OK:
            raise RuntimeError(
                _resolve_error_message(
                    client,
                    base_id.id,
                    table_id.id,
                    response.status_code,
                    response.text,
                )
            )
