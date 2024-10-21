import urllib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, Union

import httpx
import pydantic

from lutraai.augmented_request_client import AsyncAugmentedTransport
from lutraai.decorator import purpose
from lutraai.requests import raise_error_text


@dataclass
class HubSpotObjectType:
    """name represents the name of object in HubSpot CRM."""

    name: Literal[
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
    name: str


@dataclass
class _HubSpotPropertiesSchema:
    """The schema for a HubSpot object's properties.

    See https://developers.hubspot.com/docs/api/crm/properties#retrieve-properties
    """

    properties: dict[str, dict[str, Any]]
    """
    Mapping from property name to property schema. 
    """


async def _get_hubspot_properties_schema(
    object_type: HubSpotObjectType,
) -> _HubSpotPropertiesSchema:
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.get(
            f"https://api.hubapi.com/crm/v3/properties/{object_type.name}"
        )
        await raise_error_text(response)
        await response.aread()
        return _HubSpotPropertiesSchema(
            properties={prop["name"]: prop for prop in response.json()["results"]}
        )


def _get_all_property_names(schema: _HubSpotPropertiesSchema) -> list[str]:
    return list(schema.properties.keys())


@dataclass
class HubSpotPropertyValue:
    """A property value from HubSpot.

    `value` property can only be a string, int, float, or datetime.
    """

    value: Any


@dataclass
class HubSpotContact:
    """The `additional_properties` field stores any additional properties that are
    available in the HubSpot contact system that callers can ask for. If found, they
    will be found here.

    You MUST specify all the fields when constructing this object.
    """

    firstname: str
    lastname: str
    email: str
    hs_object_id: str
    last_modified_date: datetime
    additional_properties: Dict[str, HubSpotPropertyValue]
    created_at: datetime
    updated_at: datetime
    archived: bool


@dataclass
class HubSpotPaginationToken:
    token: str


def _coerce_properties_to_lutra(
    properties: Mapping[str, Union[str, int, float, date, datetime, bool]],
    schema: _HubSpotPropertiesSchema,
) -> Dict[str, HubSpotPropertyValue]:
    coerced_properties: Dict[str, HubSpotPropertyValue] = {}
    for name, value in properties.items():
        property_schema = schema.properties.get(name)
        if property_schema is None:
            # Fall back to `str` if the property is unknown.
            c_value = str(value)
        else:
            match property_schema["type"].lower():
                case "bool":
                    if value == "":
                        c_value = None  # The value is an empty string when the boolean is not set
                    else:
                        # HubSpot boolean properties seem to come as the strings "true" and "false," but we
                        # can't find a guarantee that they do, so use Pydantic parsing to accept many boolean
                        # representations just in case.
                        c_value = pydantic.parse_obj_as(bool, value)
                case "date":
                    if isinstance(value, datetime):
                        c_value = value
                    elif isinstance(value, str):
                        # The value is an empty string when the date is not set
                        c_value = date.fromisoformat(value) if value else None
                    else:
                        raise ValueError(
                            f"Unexpected date format: {value} ({type(value)})"
                        )
                case "datetime":
                    if isinstance(value, datetime):
                        c_value = value
                    elif isinstance(value, str):
                        # The value is an empty string when the date is not set
                        c_value = datetime.fromisoformat(value) if value else None
                    else:
                        raise ValueError(
                            f"Unexpected datetime format: {value} ({type(value)})"
                        )
                case "number":
                    if isinstance(value, str):
                        if "." in value:
                            c_value = float(value)
                        elif (
                            value == ""
                        ):  # The value is an empty string when the number is not set. Default to 0.
                            c_value = 0
                        else:
                            c_value = int(value)
                    elif isinstance(value, int | float):
                        c_value = value
                    else:
                        c_value = float(value)
                case _:
                    # Coerce to string for other/unknown types.
                    c_value = str(value)

        if c_value is not None:
            coerced_properties[name] = HubSpotPropertyValue(value=c_value)

    return coerced_properties


def _coerce_value_to_hubspot(
    name: str,
    value: Any,
    schema: _HubSpotPropertiesSchema,
) -> Union[str, int, bool]:
    property_schema = schema.properties.get(name)
    if property_schema is None:
        # Fall back to `str` if the property is unknown.
        return str(value)
    match property_schema["type"].lower():
        case "bool":
            # Because `value` comes from Lutra's codegen, we try to accept many representations of
            # boolean, using Pydantic's tolerant logic. The HubSpot API seems to accept boolean
            # values in the JSON request.
            return pydantic.parse_obj_as(bool, value)
        case "date":
            if isinstance(value, date):
                return value.isoformat()
            else:
                raise ValueError(f"Unexpected date format: {value} ({type(value)})")
        case "datetime":
            if isinstance(value, datetime):
                return int(value.timestamp() * 1000)
            else:
                raise ValueError(f"Unexpected datetime format: {value} ({type(value)})")
        case _:
            # Coerce to string for other/unknown types.
            return str(value)


def _coerce_properties_to_hubspot(
    properties: Mapping[
        str, Union[str, int, float, date, datetime, bool, HubSpotPropertyValue]
    ],
    schema: _HubSpotPropertiesSchema,
) -> Dict[str, Union[str, int, bool]]:
    coerced_properties = {}
    for name, value in properties.items():
        if isinstance(value, HubSpotPropertyValue):
            value = value.value
        coerced_properties[name] = _coerce_value_to_hubspot(
            name=name,
            value=value,
            schema=schema,
        )

    return coerced_properties


def _get_datetime_with_fallback(api_item: Dict[str, Any], key: str) -> datetime:
    # Note: `x.get(y) or z` is safer than `x.get(y, z)` in the case that `x[y]` is present and `None`.
    return datetime.fromisoformat(api_item.get(key) or "1970-01-01T00:00:00Z")


def _parse_hubspot_contact(
    api_item: Dict[str, Any], properties_schema: _HubSpotPropertiesSchema
) -> HubSpotContact:
    properties = api_item.get("properties") or {}
    return HubSpotContact(
        created_at=_get_datetime_with_fallback(api_item, "createdAt"),
        updated_at=_get_datetime_with_fallback(api_item, "updatedAt"),
        archived=api_item.get("archived") or False,
        firstname=properties.get("firstname") or "",
        lastname=properties.get("lastname") or "",
        email=properties.get("email") or "",
        hs_object_id=properties.get("hs_object_id") or "",
        # TODO: Verify if "lastmodifieddate" is correct here.
        # It seems that "lastmodifieddate" is defined on Contacts, but not other object types, and
        # there is also "hs_lastmodifieddate" that is defined on other object types but not
        # Contacts.
        last_modified_date=_get_datetime_with_fallback(properties, "lastmodifieddate"),
        additional_properties=_coerce_properties_to_lutra(
            {key: val for key, val in properties.items() if val is not None},
            schema=properties_schema,
        ),
    )


async def _list_contacts(
    schema: _HubSpotPropertiesSchema,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotContact], Optional[HubSpotPaginationToken]]:
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    params: Dict[str, Any] = {"limit": 100}
    if pagination_token:
        params["after"] = pagination_token.token
    params["properties"] = _get_all_property_names(schema)
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.get(url, params=params)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    contacts = [
        _parse_hubspot_contact(item, schema) for item in data.get("results") or []
    ]
    token = data.get("paging", {}).get("next", {}).get("after")
    next_pagination_token = HubSpotPaginationToken(token=token) if token else None

    return contacts, next_pagination_token


@purpose("Create contacts.")
async def hubspot_create_contacts(contacts: Sequence[HubSpotContact]) -> List[str]:
    """
    Create multiple contacts in HubSpot using the batch API.

    Args:
        contacts: A list of HubSpotContact objects to be created.

    Returns:
        A list of strings, where each string is the ID of a created contact.
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("CONTACTS"))

    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"

    # Prepare the payload from the contacts list
    contacts_payload = []
    for contact in contacts:
        properties: Dict[str, Any] = {
            "firstname": contact.firstname,
            "lastname": contact.lastname,
            "email": contact.email,
        }
        additional_properties = _coerce_properties_to_hubspot(
            contact.additional_properties,
            schema=schema,
        )
        properties.update(additional_properties)
        contact_data = {
            "properties": properties,
        }
        contacts_payload.append(contact_data)

    payload = {"inputs": contacts_payload}

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json=payload)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    # Extract and return the IDs of the created contacts
    return [result["id"] for result in data["results"]]


@purpose("Update contacts.")
async def hubspot_update_contacts(
    contact_updates: Mapping[
        str,
        Sequence[
            Tuple[
                str, Union[str, int, float, date, datetime, bool, HubSpotPropertyValue]
            ]
        ],
    ],
) -> List[str]:
    """Update multiple contacts in HubSpot.

    contact_updates is a dict mapping contact id to a list of tuples with the property names to update, and their new values.

    Returns:
        Contact IDs that have been updated.

    The property names MUST be one of the following:

    Default Properties of Type String:
    adopter_category: An assessment of whether the contact is likely to use our product.
    company_size: Contact's company size.
    date_of_birth: Contact's date of birth.
    degree: Contact's degree.
    field_of_study: Contact's field of study.
    first_conversion_event_name: First form this contact submitted.
    gender: Contact's gender.
    job_function: Contact's job function.
    jobtitle: A contact's job title.
    hs_lead_status: The contact's sales, prospecting, or outreach status.
    hs_persona: A contact's persona.
    lifecyclestage: The qualification of contacts to sales readiness.
    email: A contact's email address.
    mobilephone: A contact's mobile phone number.
    phone: A contact's primary phone number.
    city: A contact's city of residence.
    state: The contact's state of residence.
    country: The contact's country/region of residence.
    industry: The Industry a contact is in.
    hs_linkedinid: A contact's linkedin id.

    Default Properties of Type Number:
    num_associated_deals: Count of deals associated with this contact.
    num_conversion_events: The number of forms this contact has submitted.
    num_unique_conversion_events: The number of different forms this contact has submitted.
    recent_deal_amount: Amount of last closed won deal associated with a contact.
    total_revenue: Sum of all closed-won deal revenue associated with the contact.

    Default Properties of Type Datetime:
    first_conversion_date: Date this contact first submitted a form.
    first_deal_created_date: Date first deal was created for contact.
    recent_conversion_date: The date this contact last submitted a form.
    recent_deal_close_date: Date last deal associated with contact was closed-won.
    closedate: Date the contact became a customer.

    Default Properties of Type Boolean:
    hs_email_bad_address: The email address associated with this contact is invalid.
    hs_is_contact: Is a contact, has not been deleted and is not a visitor.
    hs_is_unworked: Contact has not been assigned or has not been engaged after last owner assignment/re-assignment.
    hs_sequences_is_enrolled: A yes/no field that indicates whether the contact is currently in a Sequence.
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("CONTACTS"))

    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"

    payload = [
        {
            "id": contact_id,
            "properties": _coerce_properties_to_hubspot(
                dict(properties),
                schema=schema,
            ),
        }
        for contact_id, properties in contact_updates.items()
    ]

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json={"inputs": payload})
        await raise_error_text(response)
        await response.aread()
        data = response.json()
        return [result["id"] for result in data["results"]]


async def _search_contacts(
    filter_groups: List[Dict[str, List[Dict[str, Any]]]],
    schema: _HubSpotPropertiesSchema,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotContact], Optional[HubSpotPaginationToken]]:
    if not filter_groups:
        # The API will fail with an empty list
        return [], None
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": filter_groups,
        "properties": _get_all_property_names(schema),
        "limit": 100,
    }
    if pagination_token:
        payload["after"] = pagination_token.token
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json=payload)
        await raise_error_text(response)
        await response.aread()
        data = response.json()
        contacts = [
            _parse_hubspot_contact(item, schema) for item in data.get("results") or []
        ]
        token = data.get("paging", {}).get("next", {}).get("after")
        next_pagination_token = HubSpotPaginationToken(token=token) if token else None
        return contacts, next_pagination_token


@dataclass
class HubSpotSearchCondition:
    property_name: str
    operator: Literal[
        "EQ",
        "NEQ",
        "LT",
        "LTE",
        "GT",
        "GTE",
    ]
    value: HubSpotPropertyValue


@purpose("Search contacts.")
async def hubspot_search_contacts(
    and_conditions: List[HubSpotSearchCondition],
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotContact], Optional[HubSpotPaginationToken]]:
    """Search for HubSpot contacts
    created_after: Return contacts that were created after this datetime
    created_before: Return contacts that were created before this datetime
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("CONTACTS"))
    if created_after:
        and_conditions.append(
            HubSpotSearchCondition(
                property_name="createdate",
                operator="GTE",
                value=HubSpotPropertyValue(created_after),
            )
        )
    if created_before:
        and_conditions.append(
            HubSpotSearchCondition(
                property_name="createdate",
                operator="LTE",
                value=HubSpotPropertyValue(created_before),
            )
        )
    filters: list[dict[str, Any]] = []
    for and_condition in and_conditions:
        value = _coerce_value_to_hubspot(
            name=and_condition.property_name,
            value=and_condition.value.value,
            schema=schema,
        )
        filters.append(
            {
                "propertyName": and_condition.property_name,
                "operator": and_condition.operator,
                "value": value,
            }
        )

    if not filters:
        return await _list_contacts(schema, pagination_token)
    filter_groups = [{"filters": filters}]

    return await _search_contacts(filter_groups, schema, pagination_token)


@dataclass
class HubSpotCompany:
    """The `additional_properties` field stores any additional properties that are
    available in the HubSpot contact system that callers can ask for. If found, they
    will be found here.

    You MUST specify all the fields when constructing this object.
    """

    name: str
    domain: Optional[str]
    hs_object_id: str
    last_modified_date: datetime
    additional_properties: Dict[str, HubSpotPropertyValue]
    created_at: datetime
    updated_at: datetime
    archived: bool


def _parse_hubspot_company(
    api_item: dict, schema: _HubSpotPropertiesSchema
) -> HubSpotCompany:
    properties = api_item.get("properties") or {}
    return HubSpotCompany(
        created_at=_get_datetime_with_fallback(api_item, "createdAt"),
        updated_at=_get_datetime_with_fallback(api_item, "updatedAt"),
        archived=api_item.get("archived") or False,
        name=properties.get("name") or "",
        domain=properties.get("domain") or "",
        hs_object_id=properties.get("hs_object_id") or "",
        last_modified_date=_get_datetime_with_fallback(
            properties, "hs_lastmodifieddate"
        ),
        additional_properties=_coerce_properties_to_lutra(
            {key: val for key, val in properties.items() if val is not None},
            schema=schema,
        ),
    )


async def _list_companies(
    schema: _HubSpotPropertiesSchema,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotCompany], Optional[HubSpotPaginationToken]]:
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    params: Dict[str, Any] = {"limit": 100}
    if pagination_token:
        params["after"] = pagination_token.token
    params["properties"] = _get_all_property_names(schema)
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.get(url, params=params)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    companies = [
        _parse_hubspot_company(item, schema) for item in data.get("results") or []
    ]
    token = data.get("paging", {}).get("next", {}).get("after")
    next_pagination_token = HubSpotPaginationToken(token=token) if token else None

    return companies, next_pagination_token


@purpose("Create companies.")
async def hubspot_create_companies(companies: Sequence[HubSpotCompany]) -> List[str]:
    """
    Create multiple company in HubSpot using the batch API.

    Companies are created with just the first name, last name, and email properties.
    Further properties can be updated using the hubspot_update_companies function.

    Args:
        companies: A list of HubSpotCompany objects to be created.

    Returns:
        A list of strings, where each string is the ID of a created company.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/create"

    # Prepare the payload from the contacts list
    company_payload = []
    for company in companies:
        company_data = {
            "properties": {
                "name": company.name,
                "domain": company.domain,
            }
        }
        company_payload.append(company_data)

    payload = {"inputs": company_payload}

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json=payload)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    # Extract and return the IDs of the created company
    return [result["id"] for result in data["results"]]


@purpose("Update companies.")
async def hubspot_update_companies(
    company_updates: Mapping[
        str,
        Sequence[
            Tuple[
                str, Union[str, int, float, date, datetime, bool, HubSpotPropertyValue]
            ]
        ],
    ],
) -> List[str]:
    """Update multiple companies in HubSpot.

    company_updates is a dict mapping company id to a list of tuples with the property names to update, and their new values.

    Returns:
    Company IDs that have been updated.

    The property names to update MUST be one of the following:

    Default Properties of Type String:
    name: The name of the company or organization.
    website: The main website of the company or organization. This property is used to identify unique companies.
    domain: The domain name of the company or organization.
    industry: The type of business the company performs.
    lifecyclestage: The qualification of companies to sales readiness throughout the buying journey.
    description: A short statement about the company's mission and goals.
    phone: Company primary phone number.
    address: Street address of the company or organization, including unit number.
    address2: Additional address of the company or organization.
    city: City where the company is located.
    state: State or region in which the company or organization is located.
    country: Country in which the company or organization is located.
    zip: Postal or zip code of the company or organization.
    hs_annual_revenue_currency_code: The currency code associated with the annual revenue amount.
    owneremail: HubSpot owner email for this company or organization.
    ownername: HubSpot owner name for this company or organization.
    linkedin_company_page: The URL of the LinkedIn company page for the company or organization.
    facebook_company_page: The URL of the Facebook company page for the company or organization.

    Default Properties of Type Datetime:
    hs_createdate: The date and time at which this object was created. This value is automatically set by HubSpot and may not be modified.
    first_contact_createdate: The date that the first contact from this company entered the system, which could pre-date the company's create date.
    recent_conversion_date: The most recent conversion date across all contacts associated this company or organization.
    hs_lastmodifieddate: Most recent timestamp of any property update for this company. This includes HubSpot internal properties.

    Default Properties of Type Number:
    numberofemployees: The total number of employees who work for the company or organization.
    annualrevenue: The actual or estimated annual revenue of the company.
    hs_num_open_deals: The number of open deals associated with this company.
    total_revenue: The total amount of closed won deals.
    num_associated_contacts: The number of contacts associated with this company.
    num_associated_deals: The number of deals associated with this company.

    Default Properties of Type Boolean:
    hs_is_target_account: Identifies whether this company is being marketed and sold to as part of your account-based strategy.
    is_public: Indicates if the company is publicly traded.
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("COMPANIES"))
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/update"
    payload = [
        {
            "id": company_id,
            "properties": _coerce_properties_to_hubspot(
                dict(properties),
                schema=schema,
            ),
        }
        for company_id, properties in company_updates.items()
    ]
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json={"inputs": payload})
        await raise_error_text(response)
        await response.aread()
        data = response.json()
        return [result["id"] for result in data["results"]]


@purpose("Search companies.")
async def hubspot_search_companies(
    and_conditions: List[HubSpotSearchCondition],
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotCompany], Optional[HubSpotPaginationToken]]:
    """
    Search for companies in HubSpot CRM.
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("COMPANIES"))

    # Construct the filters based on the search criteria
    filters = []
    for and_condition in and_conditions:
        value = _coerce_value_to_hubspot(
            name=and_condition.property_name,
            value=and_condition.value.value,
            schema=schema,
        )
        filters.append(
            {
                "propertyName": and_condition.property_name,
                "operator": and_condition.operator,
                "value": value,
            }
        )

    if not filters:
        return await _list_companies(schema, pagination_token)

    url = "https://api.hubapi.com/crm/v3/objects/companies/search"

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": _get_all_property_names(schema),
    }
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json=payload)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    companies = [
        _parse_hubspot_company(item, schema) for item in data.get("results") or []
    ]
    token = data.get("paging", {}).get("next", {}).get("after")
    next_pagination_token = HubSpotPaginationToken(token=token) if token else None
    return companies, next_pagination_token


@dataclass
class HubSpotDeal:
    """The `additional_properties` field stores any additional properties that are
    available in the HubSpot deal system that callers can ask for.
    """

    dealname: str
    dealstage: str
    closedate: Optional[datetime]
    amount: float
    hs_object_id: str
    last_modified_date: datetime
    additional_properties: Dict[str, HubSpotPropertyValue]
    created_at: datetime
    updated_at: datetime
    archived: bool


def _parse_hubspot_deal(
    api_item: dict, schema: _HubSpotPropertiesSchema
) -> HubSpotDeal:
    properties = api_item.get("properties") or {}
    return HubSpotDeal(
        created_at=_get_datetime_with_fallback(api_item, "createdAt"),
        updated_at=_get_datetime_with_fallback(api_item, "updatedAt"),
        archived=api_item.get("archived") or False,
        dealname=properties.get("dealname") or "",
        dealstage=properties.get("dealstage") or "",
        closedate=(
            datetime.fromisoformat(properties["closedate"])
            if properties.get("closedate")
            else None
        ),
        amount=float(properties.get("amount") or 0),
        hs_object_id=properties.get("hs_object_id") or "",
        last_modified_date=_get_datetime_with_fallback(
            properties, "hs_lastmodifieddate"
        ),
        additional_properties=_coerce_properties_to_lutra(
            {key: val for key, val in properties.items() if val is not None},
            schema=schema,
        ),
    )


async def _list_deals(
    schema: _HubSpotPropertiesSchema,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotDeal], Optional[HubSpotPaginationToken]]:
    url = "https://api.hubapi.com/crm/v3/objects/deals"
    params = {"properties": _get_all_property_names(schema), "limit": 100}
    if pagination_token:
        params["after"] = pagination_token.token

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.get(url, params=params)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    deals = [_parse_hubspot_deal(item, schema) for item in data.get("results") or []]
    token = data.get("paging", {}).get("next", {}).get("after")
    next_pagination_token = HubSpotPaginationToken(token=token) if token else None

    return deals, next_pagination_token


@purpose("Create deals.")
async def hubspot_create_deals(deals: Sequence[HubSpotDeal]) -> List[str]:
    """
    Create multiple deals in HubSpot using the batch API.

    Deals are created with the properties specified in the HubSpotDeal class.
    Additional properties can be updated using a separate update function if needed.

    Args:
        deals: A list of HubSpotDeal objects to be created.

    Returns:
        A list of strings, where each string is the ID of a created deal.
    """
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch/create"

    # Prepare the payload from the deals list
    deal_payload = []
    for deal in deals:
        deal_data = {
            "properties": {
                "dealname": deal.dealname,
                "dealstage": deal.dealstage,
                "closedate": deal.closedate.isoformat() if deal.closedate else None,
                "amount": str(deal.amount),  # Assuming amount is a numeric field
            }
        }
        deal_payload.append(deal_data)

    payload = {"inputs": deal_payload}

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json=payload)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    return [result["id"] for result in data["results"]]


@purpose("Update deals.")
async def hubspot_update_deals(
    deal_updates: Mapping[
        str,
        Sequence[
            Tuple[
                str, Union[str, int, float, date, datetime, bool, HubSpotPropertyValue]
            ]
        ],
    ],
) -> List[str]:
    """Update multiple Deals in HubSpot.

    deal_updates is a dict mapping deal id to a list of tuples with the property names to update, and their new values.

    Returns:
    Deal IDs that have been updated.

    The property names to update MUST be one of the following:

    Default Properties of Type Number:
    amount_in_home_currency: The deal amount in your company's currency, using the exchange rate.
    days_to_close: The number of days the deal took to close.
    hs_acv: The annual contract value of this deal.
    hs_arr: The annual recurring revenue of this deal.
    hs_deal_stage_probability: The probability a deal will close, based on the deal stage probability setting.
    hs_likelihood_to_close: HubSpot predicted likelihood of the deal to close by the close date.
    hs_mrr: The monthly recurring revenue of this deal.
    hs_object_id: The unique ID for this deal record.
    amount: The total amount of the deal.
    num_associated_contacts: The number of contacts associated with this deal.

    Default Properties of Type String:
    deal_currency_code: Currency code for the deal.
    dealname: The name given to this deal.
    dealstage: The stage of the deal, categorizing and tracking the progress.
    pipeline: The pipeline the deal is in, determining the stages available for the deal.
    description: Description of the deal.
    closed_lost_reason: Reason why this deal was lost.
    closed_won_reason: Reason why this deal was won.

    Default Properties of Type Datetime:
    closedate: Date the deal was closed, set automatically by HubSpot.
    createdate: Date the deal was created, set automatically by HubSpot.
    hs_closed_won_date: Returns the closedate if the deal is closed won.
    hs_lastmodifieddate: The most recent timestamp of any property update for this deal.

    Default Properties of Type Boolean:
    hs_is_closed: True if the deal was won or lost.
    hs_is_closed_won: True if the deal is in the closed-won state.
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("DEALS"))
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch/update"
    payload = [
        {
            "id": deal_id,
            "properties": _coerce_properties_to_hubspot(
                dict(properties),
                schema=schema,
            ),
        }
        for deal_id, properties in deal_updates.items()
    ]
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json={"inputs": payload})
        await raise_error_text(response)
        await response.aread()
        data = response.json()
        return [result["id"] for result in data["results"]]


@purpose("Search deals.")
async def hubspot_search_deals(
    and_conditions: List[HubSpotSearchCondition],
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[List[HubSpotDeal], Optional[HubSpotPaginationToken]]:
    """
    Search for HubSpot deals based on various criteria.

    Default properties will always be fetched. However, properties with no values will not be in the additional_properties
    dict. You MUST check whether the property exists in additional_properties before using it.
    """
    schema = await _get_hubspot_properties_schema(HubSpotObjectType("DEALS"))
    filters = []
    for and_condition in and_conditions:
        value = _coerce_value_to_hubspot(
            name=and_condition.property_name,
            value=and_condition.value.value,
            schema=schema,
        )
        filters.append(
            {
                "propertyName": and_condition.property_name,
                "operator": and_condition.operator,
                "value": value,
            }
        )

    if not filters:
        return await _list_deals(schema, pagination_token)

    url = "https://api.hubapi.com/crm/v3/objects/deals/search"

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": _get_all_property_names(schema),
    }

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = await client.post(url, json=payload)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

    deals = [_parse_hubspot_deal(item, schema) for item in data.get("results") or []]
    token = data.get("paging", {}).get("next", {}).get("after")
    next_pagination_token = HubSpotPaginationToken(token=token) if token else None

    return deals, next_pagination_token


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


@purpose("Fetch associated object IDs.")
async def hubspot_fetch_associated_object_ids(
    source_object_type: HubSpotObjectType,
    target_object_type: HubSpotObjectType,
    source_object_id: str,
) -> List[str]:
    """
    Returns the IDs of target objects associated with the source object
    using the HubSpot association API. You must use this to find HubSpot
    objects that are associated to each other.
    """
    source_type_name = _HUBSPOT_OBJECT_TYPE_IDS[source_object_type.name]
    target_type_name = _HUBSPOT_OBJECT_TYPE_IDS[target_object_type.name]
    url = f"https://api.hubapi.com/crm/v4/associations/{source_type_name}/{target_type_name}/batch/read"
    params = {"inputs": [{"id": source_object_id}]}

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = await client.post(url, json=params)
        await raise_error_text(response)
        await response.aread()
        data = response.json()

        if results := data.get("results", []):
            return [
                associated_object["toObjectId"]
                for associated_object in results[0].get("to", [])
            ]

    return []


ASSOCIATION_TYPE_IDS = {
    "CONTACT_TO_CONTACT": 449,
    "CONTACT_TO_COMPANY": 279,
    "CONTACT_TO_PRIMARY_COMPANY": 1,
    "CONTACT_TO_DEAL": 4,
    "COMPANY_TO_COMPANY": 450,
    "COMPANY_TO_CONTACT": 280,
    "COMPANY_TO_DEAL": 342,
}


@dataclass
class HubSpotAssociationType:
    type: Literal[
        "CONTACT_TO_CONTACT",
        "CONTACT_TO_COMPANY",
        "CONTACT_TO_PRIMARY_COMPANY",
        "CONTACT_TO_DEAL",
        "COMPANY_TO_COMPANY",
        "COMPANY_TO_CONTACT",
        "COMPANY_TO_DEAL",
    ]


@purpose("Create association between object IDs.")
async def hubspot_create_association_between_object_ids(
    association_type: HubSpotAssociationType,
    source_object_type: HubSpotObjectType,
    source_object_id: str,
    target_object_type: HubSpotObjectType,
    target_object_id: str,
):
    """
    Creates an association between the source and target objects in HubSpot.
    """
    source_type_name = _HUBSPOT_OBJECT_TYPE_IDS[source_object_type.name]
    target_type_name = _HUBSPOT_OBJECT_TYPE_IDS[target_object_type.name]
    url = f"https://api.hubapi.com/crm/v4/associations/{source_type_name}/{target_type_name}/batch/create"
    params = {
        "inputs": [
            {
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": ASSOCIATION_TYPE_IDS.get(
                            association_type.type
                        ),
                    }
                ],
                "from": {
                    "id": source_object_id,
                },
                "to": {
                    "id": target_object_id,
                },
            }
        ]
    }

    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = await client.post(url, json=params)
        await raise_error_text(response)


async def _merge_objects(url: str, primary_object_id: str, object_to_merge_id: str):
    params = {
        "objectIdToMerge": object_to_merge_id,
        "primaryObjectId": primary_object_id,
    }
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = await client.post(url, json=params)
        await raise_error_text(response)


@purpose("Merge contacts.")
async def hubspot_merge_contacts(primary_contact_id: str, contact_to_merge_id: str):
    """Merge contact_to_merge_id with primary_contact_id, retaining primary contact"""
    url = "https://api.hubapi.com/crm/v3/objects/contacts/merge"
    await _merge_objects(url, primary_contact_id, contact_to_merge_id)


@purpose("Merge companies.")
async def hubspot_merge_companies(primary_company_id: str, company_to_merge_id: str):
    """Merge company_to_merge with primary_company, retaining primary company"""
    url = "https://api.hubapi.com/crm/v3/objects/companies/merge"
    await _merge_objects(url, primary_company_id, company_to_merge_id)


@purpose("Fetch HubSpot List.")
async def hubspot_list_memberships(
    list_name: str, object_type: HubSpotObjectType
) -> Tuple[List[str], Optional[HubSpotPaginationToken]]:
    """Returns object_ids associated with the HubSpot List object."""
    object_type_id = _HUBSPOT_OBJECT_TYPE_IDS[object_type.name]
    escaped_list_name = urllib.parse.quote(list_name, safe="")
    url = f"https://api.hubapi.com/crm/v3/lists/object-type-id/{object_type_id}/name/{escaped_list_name}"
    object_ids = []
    next_pagination_token = None
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = await client.get(url)
        await response.aread()
        await raise_error_text(response)
        data = response.json()
        if list_data := data.get("list"):
            list_id = list_data["listId"]
            memberships_response = await client.get(
                f"https://api.hubapi.com/crm/v3/lists/{list_id}/memberships"
            )
            await raise_error_text(memberships_response)
            await memberships_response.aread()
            membership_data = memberships_response.json()
            token = data.get("paging", {}).get("next", {}).get("after")
            next_pagination_token = (
                HubSpotPaginationToken(token=token) if token else None
            )
            if results := membership_data.get("results"):
                object_ids = [result.get("recordId") for result in results]
    return object_ids, next_pagination_token
