from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple, Union, Literal

import httpx

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class HubSpotContactProperties:
    """Represents the properties of a HubSpot contact.

    The `additional_properties` field stores any additional properties that are
    available in the HubSpot contact system that callers can ask for. If found, they
    will be found here.
    """

    first_name: str
    last_name: str
    email: str
    hs_object_id: str
    last_modified_date: datetime

    additional_properties: Dict[str, str]


@dataclass
class HubSpotContact:
    id: str
    properties: HubSpotContactProperties
    createdAt: datetime
    updatedAt: datetime
    archived: bool


def list_contacts(
    limit: int = 100, after: Optional[str] = None
) -> Tuple[List[HubSpotContact], Optional[str]]:
    """
    Fetch the list of contacts from HubSpot.

    No additional properties are returned from this API. Use search_contacts to retrieve
    specific contacts with additional properties.

    Args:
        limit: The maximum number of results to display per page. The maximum value for this is 100.
        after: Cursor for pagination.

    Returns:
        A tuple of a list of HubSpotContact objects and the next 'after' cursor, if
            available. If the next 'after' cursor is None, there is no more data to get.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    params = {}
    if limit:
        params["limit"] = limit
    if after:
        params["after"] = after

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    contacts = []
    for item in data["results"]:
        properties = item["properties"]
        contact_properties = HubSpotContactProperties(
            first_name=properties["firstname"],
            last_name=properties["lastname"],
            email=properties.get("email", ""),
            hs_object_id=properties["hs_object_id"],
            last_modified_date=datetime.fromisoformat(properties["lastmodifieddate"]),
            additional_properties={},
        )
        contact = HubSpotContact(
            id=item["id"],
            properties=contact_properties,
            createdAt=datetime.fromisoformat(item["createdAt"]),
            updatedAt=datetime.fromisoformat(item["updatedAt"]),
            archived=item["archived"],
        )
        contacts.append(contact)

    next_after = (
        data["paging"]["next"]["after"]
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return contacts, next_after


def create_contacts(contacts: List[HubSpotContact]) -> List[str]:
    """
    Create multiple contacts in HubSpot using the batch API.

    Contacts are created with just the first name, last name, and email properties.
    Further properties can be updated using the update_contacts function.

    Args:
        contacts: A list of HubSpotContact objects to be created.

    Returns:
        A list of strings, where each string is the ID of a created contact.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"

    # Prepare the payload from the contacts list
    contacts_payload = []
    for contact in contacts:
        contact_data = {
            "properties": {
                "firstname": contact.properties.first_name,
                "lastname": contact.properties.last_name,
                "email": contact.properties.email,
            }
        }
        contacts_payload.append(contact_data)

    payload = {"inputs": contacts_payload}

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    # Extract and return the IDs of the created contacts
    return [result["id"] for result in data["results"]]


@dataclass
class HubSpotContactUpdate:
    """The data required to update a HubSpot contact.

    Attributes:
        id: The HubSpot ID of the contact to update.
        updated_properties: A dictionary of property names and values to update for the
            contact. Only properties that are to be updated need to be included.

    """

    id: str
    updated_properties: Dict[str, str]


def update_contacts(contacts: List[HubSpotContactUpdate]) -> List[str]:
    """
    Update multiple contacts in HubSpot.

    Args:
        contacts: A list of HubSpotContactUpdate objects to be updated.

    Returns:
        A list of strings, where each string is the HubSpot ID of an updated contact.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"

    # Prepare the payload from the contacts list
    contacts_payload = []
    for contact in contacts:
        contact_data = {
            "id": contact.id,  # Contact ID is required for updating
            "properties": contact.updated_properties,
        }
        contacts_payload.append(contact_data)

    payload = {"inputs": contacts_payload}

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    # Extract and return the IDs of the updated contacts
    return [result["id"] for result in data["results"]]


def search_contacts(
    search_criteria: Dict[str, str],
    additional_properties: Optional[Sequence[str]] = None,
) -> List[HubSpotContact]:
    """
    Search for HubSpot contacts based on various criteria.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "firstname", "email") and values are the search values for those properties.
        additional_properties: A sequence of property names to fetch from found
            contacts. If present, the corresponding values will be provided in the
            HubSpotContactProperties additional_properties field. Standard hubspot
            properties are available, but users must know the names of custom properties
            if they are to be found.

    Returns:
        List[HubSpotContact]: A list of HubSpotContact objects matching the search
            criteria.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"

    # Construct the filters based on the search criteria
    filters = []
    for property_name, value in search_criteria.items():
        filters.append(
            {"propertyName": property_name, "operator": "EQ", "value": value}
        )

    properties = ["firstname", "lastname", "email", "lastmodifieddate"]
    if additional_properties:
        properties = list({*properties, *additional_properties})
    # Prepare the request body with the filters
    payload = {"filterGroups": [{"filters": filters}], "properties": properties}

    contacts = []
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    for item in data.get("results", []):
        property_values = item.get("properties", {})
        additional_property_values = {}
        if additional_properties:
            for addl in additional_properties:
                val = property_values.get(addl, None)
                if val:
                    additional_property_values[addl] = val

        contact_properties = HubSpotContactProperties(
            first_name=property_values.get("firstname", ""),
            last_name=property_values.get("lastname", ""),
            email=property_values.get("email", ""),
            hs_object_id=item["id"],
            last_modified_date=datetime.fromisoformat(
                property_values.get("lastmodifieddate", "1970-01-01T00:00:00Z")
            ),
            additional_properties=additional_property_values,
        )

        contact = HubSpotContact(
            id=item["id"],
            properties=contact_properties,
            createdAt=datetime.fromisoformat(
                item.get("createdAt", "1970-01-01T00:00:00Z")
            ),
            updatedAt=datetime.fromisoformat(
                item.get("updatedAt", "1970-01-01T00:00:00Z")
            ),
            archived=item.get("archived", False),
        )
        contacts.append(contact)

    return contacts


@dataclass
class HubSpotCompanyProperties:
    """Represents the properties of a HubSpot company.

    The `additional_properties` field stores any additional properties that are
    available in the HubSpot company system that callers can ask for. If found, they
    will be found here.
    """

    name: str
    domain: str
    hs_object_id: str
    last_modified_date: datetime

    additional_properties: Dict[str, str]


@dataclass
class HubSpotCompany:
    id: str
    properties: HubSpotCompanyProperties
    createdAt: datetime
    updatedAt: datetime
    archived: bool


def list_companies(
    limit: int = 100,
    after: Optional[str] = None,
    additional_properties: Optional[Sequence[str]] = None,
) -> Tuple[List[HubSpotCompany], Optional[str]]:
    """
    Fetch the list of companies from HubSpot.

    Args:
        limit: The maximum number of results to display per page.
        after: Cursor for pagination.
        additional_properties: A sequence of property names to fetch from found
            companies. If present, the corresponding values will be provided in the
            HubSpotCompanyProperties additional_properties field.

    Returns:
        A tuple of a list of HubSpotCompany objects and the next 'after' cursor, if
            available. If the next 'after' cursor is None, there is no more data to get.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    params = {
        "limit": limit,
        "properties": [
            "name",
            "domain",
            "city",
            "state",
            "phone",
            "industry",
            "hs_object_id",
            "lastmodifieddate",
        ],
    }
    if after:
        params["after"] = after
    if additional_properties:
        params["properties"].extend(additional_properties)

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    companies = []
    for item in data["results"]:
        property_values = item.get("properties", {})
        additional_property_values = {}
        if additional_properties:
            for addl in additional_properties:
                val = property_values.get(addl, None)
                if val:
                    additional_property_values[addl] = val

        company_properties = HubSpotCompanyProperties(
            name=property_values["name"],
            domain=property_values["domain"],
            hs_object_id=item["id"],
            last_modified_date=datetime.fromisoformat(
                property_values.get("lastmodifieddate", "1970-01-01T00:00:00Z")
            ),
            additional_properties=additional_property_values,
        )
        company = HubSpotCompany(
            id=item["id"],
            properties=company_properties,
            createdAt=datetime.fromisoformat(item["createdAt"]),
            updatedAt=datetime.fromisoformat(item["updatedAt"]),
            archived=item["archived"],
        )
        companies.append(company)

    next_after = (
        data["paging"]["next"]["after"]
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return companies, next_after


def search_companies(
    search_criteria: Dict[str, str],
    additional_properties: Optional[Sequence[str]] = None,
) -> List[HubSpotCompany]:
    """
    Search for companies in HubSpot CRM based on various criteria.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "name", "domain") and values are the search values for those properties.
        additional_properties: A sequence of property names to fetch from found
            companies. If present, the corresponding values will be provided in the
            HubSpotCompanyProperties additional_properties field. Standard HubSpot
            properties are available, but users must know the names of custom properties
            if they are to be found.

    Returns:
        List[HubSpotCompany]: A list of HubSpotCompany objects matching the search
            criteria.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"

    # Construct the filters based on the search criteria
    filters = []
    for property_name, value in search_criteria.items():
        filters.append(
            {"propertyName": property_name, "operator": "EQ", "value": value}
        )

    properties = [
        "name",
        "domain",
    ]
    if additional_properties:
        properties = list({*properties, *additional_properties})

    payload = {"filterGroups": [{"filters": filters}], "properties": properties}

    companies = []
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    for item in data.get("results", []):
        property_values = item.get("properties", {})
        additional_property_values = {}
        if additional_properties:
            for addl in additional_properties:
                val = property_values.get(addl, None)
                if val:
                    additional_property_values[addl] = val

        company_properties = HubSpotCompanyProperties(
            name=property_values.get("name", ""),
            domain=property_values.get("domain", ""),
            hs_object_id=item["id"],
            last_modified_date=datetime.fromisoformat(
                property_values.get("lastmodifieddate", "1970-01-01T00:00:00Z")
            ),
            additional_properties=additional_property_values,
        )

        company = HubSpotCompany(
            id=item["id"],
            properties=company_properties,
            createdAt=datetime.fromisoformat(
                item.get("createdAt", "1970-01-01T00:00:00Z")
            ),
            updatedAt=datetime.fromisoformat(
                item.get("updatedAt", "1970-01-01T00:00:00Z")
            ),
            archived=item.get("archived", False),
        )
        companies.append(company)

    return companies


@dataclass
class HubSpotDealProperties:
    """Represents the properties of a HubSpot deal."""

    amount: int
    close_date: Optional[datetime]
    create_date: datetime
    deal_name: str
    deal_stage: str
    pipeline: str
    additional_properties: Dict[str, str]


@dataclass
class HubSpotDeal:
    """Represents a deal in HubSpot."""

    id: str
    properties: HubSpotDealProperties
    createdAt: datetime
    updatedAt: datetime
    archived: bool


def _parse_deal(data: dict) -> HubSpotDeal:
    properties = data["properties"]

    deal_properties = HubSpotDealProperties(
        amount=int(properties["amount"]),
        close_date=(
            datetime.fromisoformat(properties["closedate"])
            if properties["closedate"]
            else None
        ),
        create_date=datetime.fromisoformat(properties["createdate"]),
        deal_name=properties["dealname"],
        deal_stage=properties["dealstage"],
        pipeline=properties["pipeline"],
        additional_properties={
            k: v
            for k, v in properties.items()
            if k
            not in [
                "amount",
                "closedate",
                "createdate",
                "dealname",
                "dealstage",
                "pipeline",
            ]
            and v is not None
        },
    )

    return HubSpotDeal(
        id=data["id"],
        properties=deal_properties,
        createdAt=datetime.fromisoformat(data["createdAt"]),
        updatedAt=datetime.fromisoformat(data["updatedAt"]),
        archived=data["archived"],
    )


def fetch_deal_by_id(deal_id: str) -> HubSpotDeal:
    """Fetch a deal by its ID from HubSpot."""
    url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()

    deal_properties = data.get("properties", {})
    if not deal_properties:
        raise ValueError(f"Deal with ID {deal_id} not found")

    return _parse_deal(data)


def _parse_company(data: dict) -> HubSpotDeal:
    properties = data["properties"]

    deal_properties = HubSpotCompanyProperties(
        name=properties["name"],
        domain=properties["domain"],
        hs_object_id=properties["hs_object_id"],
        last_modified_date=datetime.fromisoformat(properties["hs_lastmodifieddate"]),
        additional_properties={
            key: value
            for key, value in data["properties"].items()
            if key not in ["name", "domain", "hs_object_id", "lastmodifieddate"]
        },
    )

    return HubSpotCompany(
        id=data["id"],
        properties=deal_properties,
        createdAt=datetime.fromisoformat(data["createdAt"]),
        updatedAt=datetime.fromisoformat(data["updatedAt"]),
        archived=data["archived"],
    )


def fetch_company_by_id(company_id: str) -> HubSpotCompany:
    """
    Fetch a company from HubSpot CRM based on a company ID.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()
        return _parse_company(data)


_HUBSPOT_OBJECT_TYPE_IDS = dict(
    CONTACTS="0-1",
    COMPANIES="0-2",
    DEALS="0-3",
    TICKETS="0-5",
    CALLS="0-48",
    EMAILS="0-49",
    MEETINGS="0-47",
    NOTES="0-4",
    TASKS="0-27",
    PRODUCTS="0-7",
    INVOICES="0-52",
    LINE_ITEMS="0-8",
    PAYMENTS="0-101",
    QUOTES="0-14",
    SUBSCRIPTIONS="0-69",
    COMMUNICATIONS="0-18",
    POSTAL_MAIL="0-116",
    MARKETING_EVENTS="0-54",
    FEEDBACK_SUBMISSIONS="0-19",
)


@dataclass
class HubSpotObjectType:
    id: Literal[
        "CONTACTS",
        "COMPANIES",
        "DEALS",
        "TICKETS",
        "CALLS",
        "EMAILS",
        "MEETINGS",
        "NOTES",
        "TASKS",
        "PRODUCTS",
        "INVOICES",
        "LINE_ITEMS",
        "PAYMENTS",
        "QUOTES",
        "SUBSCRIPTIONS",
        "COMMUNICATIONS",
        "POSTAL_MAIL",
        "MARKETING_EVENTS",
        "FEEDBACK_SUBMISSIONS",
    ]


@dataclass
class HubSpotCustomObjectType:
    id: str


def fetch_associated_object_ids(
    source_object_type: Union[HubSpotObjectType, HubSpotCustomObjectType],
    target_object_type: Union[HubSpotObjectType, HubSpotCustomObjectType],
    source_object_id: str,
) -> List[str]:
    """
    Fetches the IDs of objects associated with a specified source object ID.
    """
    source_type_id = _HUBSPOT_OBJECT_TYPE_IDS.get(
        source_object_type.id, source_object_type.id
    )
    target_type_id = _HUBSPOT_OBJECT_TYPE_IDS.get(
        target_object_type.id, target_object_type.id
    )
    url = f"https://api.hubapi.com/crm/v4/associations/{source_type_id}/{target_type_id}/batch/read"
    params = {"inputs": [{"id": source_object_id}]}

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.post(url, json=params)
        response.raise_for_status()
        data = response.json()

        if results := data.get("results", []):
            return [
                associated_object["toObjectId"]
                for associated_object in results[0].get("to", [])
            ]

    return []
