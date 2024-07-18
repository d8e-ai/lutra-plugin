import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlparse

import httpx
import tenacity
from lutraai.augmented_request_client import AugmentedTransport
from lutraai.decorator import purpose


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
class AirtableViewID:
    """
    You must pass view id to other functions if it was successfully
    extracted from the Airtable web UI URL.
    """

    id: str


@dataclass
class AirtableRecordID:
    id: str


@tenacity.retry(
    # See https://web.archive.org/web/20240604172435/https://support.airtable.com/docs/airtable-api-common-troubleshooting
    retry=tenacity.retry_if_result(
        lambda r: r.status_code
        in {
            # Too many requests.
            429,
            # > Airtable's servers are restarting or an unexpected outage is in
            # > progress.  You should generally not receive this error, and requests are
            # > safe to retry.
            502,
            # > The server could not process your request in time. The server could be
            # > temporarily unavailable, or it could have timed out processing your
            # > request.  You should retry the request with backoffs.
            503,
        }
    ),
    # Do the same as Airtable's own client:
    # - https://github.com/Airtable/airtable.js/blob/899adb414ebc789aa3b0dcfe6c19113377315564/src/exponential_backoff_with_jitter.ts#L3-L13
    # - https://github.com/Airtable/airtable.js/blob/899adb414ebc789aa3b0dcfe6c19113377315564/src/internal_config.json#L2-L3
    wait=tenacity.wait_random_exponential(multiplier=5, max=timedelta(minutes=10)),
)
def _maybe_retry_send(client: httpx.Client, request: httpx.Request) -> httpx.Response:
    """Send a request using the client, retrying if appropriate."""
    return client.send(request)


@purpose("Parse IDs from Airtable website URLs.")
def airtable_parse_ids_from_url(
    url: str,
) -> Tuple[
    AirtableBaseID,
    Optional[AirtableTableID],
    Optional[AirtableViewID],
    Optional[AirtableRecordID],
]:
    """
    Parses Airtable IDs from an Airtable web UI URL.  The minimum required URL format is
    https://airtable.com/{base_id}, with {base_id} being mandatory.  The URL may
    optionally include {table_id}, {view_id}, and {record_id} segments in this sequence,
    but these are not required.  Any segments or query parameters beyond the specified
    record_id are ignored.

    table_id, view_id, and record_id are returned only if present in the URL;
    otherwise, None is returned for each missing ID.

    Important: The presence of table_id, view_id, or record_id is not guaranteed. They
    will only be included in the output if explicitly present in the URL. Users should
    not expect every URL to contain all IDs and should adapt their usage based on the
    specific IDs provided.
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
        view_id = match.group("view_id")
        record_id = match.group("record_id")
        return (
            AirtableBaseID(base_id),
            AirtableTableID(table_id) if table_id else None,
            AirtableViewID(view_id) if view_id else None,
            AirtableRecordID(record_id) if record_id else None,
        )
    else:
        raise ValueError(
            "does not match the expected Airtable URL format, "
            "https://airtable.com/{{base_id}}/{{table_id}}/{{view_id}}/{{record_id}}: "
            f"{url}"
        )


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
    match error := data.get("error"):
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


def _fetch_schema(client: httpx.Client, base_id: str, table_id_or_name: str) -> Any:
    data = (
        _maybe_retry_send(
            client,
            client.build_request(
                "GET", f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
            ),
        )
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
        raise ValueError(
            f"{table_id_or_name} not found in tables: {sorted(table_id_names)}"
        )
    return schema


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
            schema = _fetch_schema(client, base_id, table_id_or_name)
            return f"{msg}; schema of table `{table_id_or_name}`: {json.dumps(schema)}"
        except Exception as e:
            return f"{msg}; (error fetching schema of {table_id_or_name}: {e})"
    return msg


@dataclass
class AirtableRecord:
    record_id: AirtableRecordID
    created_time: datetime
    fields: dict[str, Any]


@dataclass
class AirtableSortField:
    field: str
    direction: Optional[Literal["asc", "desc"]]


@dataclass
class AirtablePaginationToken:
    token: str


@purpose("List records.")
def airtable_record_list(
    base_id: AirtableBaseID,
    table_id: AirtableTableID,
    view_id: Optional[AirtableViewID] = None,
    sort: Optional[list[AirtableSortField]] = None,
    filter_by_formula: Optional[str] = None,
    include_fields: Optional[set[str]] = None,
    pagination_token: Optional[AirtablePaginationToken] = None,
) -> Tuple[list[AirtableRecord], Optional[AirtablePaginationToken]]:
    """
    Returns the results of an Airtable `list records` API call, allowing selection of
    specific fields to include in the returned records and handling pagination.
    You must populate include_fields with necessary fields to access data by name in returned records.
    Before accessing a field's value in the returned record, you MUST check for the existence of the field.

    Returns:
      - A list of AirtableRecord objects.
      - A pagination token to be used in subsequent calls to retrieve additional records.
    """
    post_body = {}

    if pagination_token is not None:
        post_body["offset"] = pagination_token.token
    if include_fields is not None:
        post_body["fields"] = list(include_fields)

    if view_id is not None:
        post_body["view"] = view_id.id

    if sort is not None:
        post_body["sort"] = [
            {
                "field": sort_field.field,
                "direction": sort_field.direction,
            }
            for sort_field in sort
        ]
    if filter_by_formula is not None:
        post_body["filterByFormula"] = filter_by_formula

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        # TODO: Consider only using a POST if the query parameters are actually too
        # large. This will allow caching, because GET.

        # Listing records is safe to retry, despite being a POST.
        response = _maybe_retry_send(
            client,
            client.build_request(
                "POST",
                f"https://api.airtable.com/v0/{base_id.id}/{table_id.id}/listRecords",
                json=post_body,
            ),
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
        next_offset = data.get("offset", None)
        next_token = AirtablePaginationToken(token=next_offset) if next_offset else None

    return [
        AirtableRecord(
            record_id=AirtableRecordID(record["id"]),
            created_time=datetime.fromisoformat(record["createdTime"]),
            fields=record["fields"],
        )
        for record in data["records"]
    ], next_token


@purpose("Create a record.")
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
        # Airtable documentation seems to suggest that it is likely that errors retried
        # by _maybe_retry_send mean that the record was not created.
        response = _maybe_retry_send(
            client,
            client.build_request(
                "POST",
                f"https://api.airtable.com/v0/{base_id.id}/{table_id.id}",
                json={"fields": fields, "typecast": typecast},
            ),
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


@purpose("Update a record.")
def airtable_record_update_patch(
    base_id: AirtableBaseID,
    table_id: AirtableTableID,
    record_id: AirtableRecordID,
    fields: dict[str, Any],
    typecast: bool = True,
) -> None:
    """
    Update a record using the Airtable `update record` API call with a PATCH and override the fields it is patching.
    You are able to update fields which may not be present.

    If typecast is True, Airtable will try to convert the value to the appropriate cell value.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_airtable)
    ) as client:
        response = _maybe_retry_send(
            client,
            client.build_request(
                "PATCH",
                f"https://api.airtable.com/v0/{base_id.id}/{table_id.id}/{record_id.id}",
                json={"fields": fields, "typecast": typecast},
            ),
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
