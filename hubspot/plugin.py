from dataclasses import dataclass
from datetime import datetime
from distutils import util
from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union, Any

import httpx
from lutraai.augmented_request_client import AugmentedTransport


_CONTACT_PROPERTIES_STRING = [
    "adopter_category",
    "company_size",
    "date_of_birth",
    "degree",
    "field_of_study",
    "first_conversion_event_name",
    "gender",
    "graduation_date",
    "hs_additional_emails",
    "hs_all_assigned_business_unit_ids",
    "hs_all_contact_vids",
    "hs_analytics_first_touch_converting_campaign",
    "hs_analytics_last_touch_converting_campaign",
    "hs_avatar_filemanager_key",
    "hs_buying_role",
    "hs_calculated_form_submissions",
    "hs_calculated_merged_vids",
    "hs_calculated_mobile_number",
    "hs_calculated_phone_number",
    "hs_calculated_phone_number_area_code",
    "hs_calculated_phone_number_country_code",
    "hs_calculated_phone_number_region_code",
    "hs_clicked_linkedin_ad",
    "hs_content_membership_email",
    "hs_content_membership_notes",
    "hs_content_membership_registration_domain_sent_to",
    "hs_content_membership_status",
    "hs_conversations_visitor_email",
    "hs_email_customer_quarantined_reason",
    "hs_email_domain",
    "hs_email_hard_bounce_reason",
    "hs_email_hard_bounce_reason_enum",
    "hs_email_quarantined_reason",
    "hs_emailconfirmationstatus",
    "hs_facebook_click_id",
    "hs_facebookid",
    "hs_feedback_last_nps_follow_up",
    "hs_feedback_last_nps_rating",
    "hs_google_click_id",
    "hs_googleplusid",
    "hs_ip_timezone",
    "hs_journey_stage",
    "hs_last_sales_activity_type",
    "hs_lead_status",
    "hs_legal_basis",
    "hs_linkedin_ad_clicked",
    "hs_linkedinid",
    "hs_marketable_reason_id",
    "hs_marketable_reason_type",
    "hs_marketable_status",
    "hs_marketable_until_renewal",
    "hs_merged_object_ids",
    "hs_mobile_sdk_push_tokens",
    "hs_notes_next_activity_type",
    "hs_object_source",
    "hs_object_source_detail_1",
    "hs_object_source_detail_2",
    "hs_object_source_detail_3",
    "hs_object_source_id",
    "hs_object_source_label",
    "hs_pipeline",
    "hs_predictivescoringtier",
    "hs_sa_first_engagement_descr",
    "hs_sa_first_engagement_object_type",
    "hs_testpurge",
    "hs_testrollback",
    "hs_timezone",
    "hs_twitterid",
    "hs_unique_creation_key",
    "hs_user_ids_of_all_notification_followers",
    "hs_user_ids_of_all_notification_unfollowers",
    "hs_user_ids_of_all_owners",
    "hs_whatsapp_phone_number",
    "ip_city",
    "ip_country",
    "ip_country_code",
    "ip_latlon",
    "ip_state",
    "ip_state_code",
    "ip_zipcode",
    "job_function",
    "marital_status",
    "military_status",
    "recent_conversion_event_name",
    "relationship_status",
    "school",
    "seniority",
    "start_date",
    "work_email",
    "firstname",
    "hs_analytics_first_url",
    "twitterhandle",
    "currentlyinworkflow",
    "hs_analytics_last_url",
    "lastname",
    "salutation",
    "twitterprofilephoto",
    "email",
    "hs_persona",
    "mobilephone",
    "phone",
    "fax",
    "hs_email_last_email_name",
    "address",
    "engagements_last_meeting_booked_campaign",
    "engagements_last_meeting_booked_medium",
    "engagements_last_meeting_booked_source",
    "hubspot_owner_id",
    "owneremail",
    "ownername",
    "city",
    "hubspot_team_id",
    "linkedinbio",
    "twitterbio",
    "hs_all_owner_ids",
    "state",
    "hs_all_team_ids",
    "hs_analytics_source",
    "hs_latest_source",
    "zip",
    "country",
    "hs_all_accessible_team_ids",
    "hs_analytics_source_data_1",
    "hs_latest_source_data_1",
    "hs_analytics_source_data_2",
    "hs_language",
    "hs_latest_source_data_2",
    "hs_analytics_first_referrer",
    "jobtitle",
    "photo",
    "hs_analytics_last_referrer",
    "message",
    "lifecyclestage",
    "company",
    "website",
    "numemployees",
    "annualrevenue",
    "industry",
    "hs_predictivecontactscorebucket",
]

_CONTACT_PROPERTIES_NUMBER = [
    "days_to_close",
    "hs_count_is_unworked",
    "hs_count_is_worked",
    "hs_created_by_user_id",
    "hs_email_sends_since_last_engagement",
    "hs_first_engagement_object_id",
    "hs_has_active_subscription",
    "hs_latest_sequence_enrolled",
    "hs_object_id",
    "hs_object_source_user_id",
    "hs_pinned_engagement_id",
    "hs_predictivecontactscore_v2",
    "hs_sequences_actively_enrolled_count",
    "hs_sequences_enrolled_count",
    "hs_source_object_id",
    "hs_source_portal_id",
    "hs_time_between_contact_creation_and_deal_close",
    "hs_time_between_contact_creation_and_deal_creation",
    "hs_time_in_customer",
    "hs_time_in_evangelist",
    "hs_time_in_lead",
    "hs_time_in_marketingqualifiedlead",
    "hs_time_in_opportunity",
    "hs_time_in_other",
    "hs_time_in_salesqualifiedlead",
    "hs_time_in_subscriber",
    "hs_time_to_first_engagement",
    "hs_time_to_move_from_lead_to_customer",
    "hs_time_to_move_from_marketingqualifiedlead_to_customer",
    "hs_time_to_move_from_opportunity_to_customer",
    "hs_time_to_move_from_salesqualifiedlead_to_customer",
    "hs_time_to_move_from_subscriber_to_customer",
    "hs_updated_by_user_id",
    "hs_v2_cumulative_time_in_customer",
    "hs_v2_cumulative_time_in_evangelist",
    "hs_v2_cumulative_time_in_lead",
    "hs_v2_cumulative_time_in_marketingqualifiedlead",
    "hs_v2_cumulative_time_in_opportunity",
    "hs_v2_cumulative_time_in_other",
    "hs_v2_cumulative_time_in_salesqualifiedlead",
    "hs_v2_cumulative_time_in_subscriber",
    "hs_v2_latest_time_in_customer",
    "hs_v2_latest_time_in_evangelist",
    "hs_v2_latest_time_in_lead",
    "hs_v2_latest_time_in_marketingqualifiedlead",
    "hs_v2_latest_time_in_opportunity",
    "hs_v2_latest_time_in_other",
    "hs_v2_latest_time_in_salesqualifiedlead",
    "hs_v2_latest_time_in_subscriber",
    "lutrauserid",
    "num_associated_deals",
    "num_conversion_events",
    "num_unique_conversion_events",
    "recent_deal_amount",
    "total_revenue",
    "hs_email_delivered",
    "followercount",
    "hs_email_open",
    "hs_analytics_num_page_views",
    "hs_email_click",
    "hs_analytics_num_visits",
    "hs_email_bounce",
    "hs_analytics_num_event_completions",
    "hs_social_twitter_clicks",
    "hs_social_facebook_clicks",
    "hs_social_linkedin_clicks",
    "num_contacted_notes",
    "num_notes",
    "surveymonkeyeventlastupdated",
    "webinareventlastupdated",
    "hs_social_google_plus_clicks",
    "hs_social_num_broadcast_clicks",
    "linkedinconnections",
    "kloutscoregeneral",
    "hs_analytics_average_page_views",
    "hs_email_replied",
    "hs_analytics_revenue",
    "hubspotscore",
    "associatedcompanyid",
    "associatedcompanylastupdated",
    "hs_predictivecontactscore",
]

_CONTACT_PROPERTIES_DATETIME = [
    "first_conversion_date",
    "first_deal_created_date",
    "hs_content_membership_follow_up_enqueued_at",
    "hs_content_membership_registered_at",
    "hs_content_membership_registration_email_sent_at",
    "hs_createdate",
    "hs_date_entered_customer",
    "hs_date_entered_evangelist",
    "hs_date_entered_lead",
    "hs_date_entered_marketingqualifiedlead",
    "hs_date_entered_opportunity",
    "hs_date_entered_other",
    "hs_date_entered_salesqualifiedlead",
    "hs_date_entered_subscriber",
    "hs_date_exited_customer",
    "hs_date_exited_evangelist",
    "hs_date_exited_lead",
    "hs_date_exited_marketingqualifiedlead",
    "hs_date_exited_opportunity",
    "hs_date_exited_other",
    "hs_date_exited_salesqualifiedlead",
    "hs_date_exited_subscriber",
    "hs_document_last_revisited",
    "hs_email_recipient_fatigue_recovery_time",
    "hs_feedback_last_survey_date",
    "hs_first_outreach_date",
    "hs_first_subscription_create_date",
    "hs_last_sales_activity_date",
    "hs_last_sales_activity_timestamp",
    "hs_lastmodifieddate",
    "hs_latest_disqualified_lead_date",
    "hs_latest_open_lead_date",
    "hs_latest_qualified_lead_date",
    "hs_latest_sequence_ended_date",
    "hs_latest_sequence_enrolled_date",
    "hs_latest_sequence_finished_date",
    "hs_latest_sequence_unenrolled_date",
    "hs_latest_source_timestamp",
    "hs_latest_subscription_create_date",
    "hs_sa_first_engagement_date",
    "hs_sales_email_last_clicked",
    "hs_sales_email_last_opened",
    "hs_v2_date_entered_customer",
    "hs_v2_date_entered_evangelist",
    "hs_v2_date_entered_lead",
    "hs_v2_date_entered_marketingqualifiedlead",
    "hs_v2_date_entered_opportunity",
    "hs_v2_date_entered_other",
    "hs_v2_date_entered_salesqualifiedlead",
    "hs_v2_date_entered_subscriber",
    "hs_v2_date_exited_customer",
    "hs_v2_date_exited_evangelist",
    "hs_v2_date_exited_lead",
    "hs_v2_date_exited_marketingqualifiedlead",
    "hs_v2_date_exited_opportunity",
    "hs_v2_date_exited_other",
    "hs_v2_date_exited_salesqualifiedlead",
    "hs_v2_date_exited_subscriber",
    "hubspot_owner_assigneddate",
    "lastmodifieddate",
    "recent_conversion_date",
    "recent_deal_close_date",
    "hs_social_last_engagement",
    "hs_analytics_first_timestamp",
    "hs_email_last_send_date",
    "engagements_last_meeting_booked",
    "hs_analytics_first_visit_timestamp",
    "hs_email_last_open_date",
    "hs_latest_meeting_activity",
    "hs_sales_email_last_replied",
    "notes_last_contacted",
    "notes_last_updated",
    "notes_next_activity_date",
    "hs_analytics_last_timestamp",
    "hs_email_last_click_date",
    "hs_analytics_last_visit_timestamp",
    "hs_email_first_send_date",
    "hs_email_first_open_date",
    "hs_email_first_click_date",
    "hs_email_first_reply_date",
    "hs_email_last_reply_date",
    "closedate",
    "hs_lifecyclestage_lead_date",
    "hs_lifecyclestage_marketingqualifiedlead_date",
    "hs_lifecyclestage_opportunity_date",
    "hs_lifecyclestage_salesqualifiedlead_date",
    "createdate",
    "hs_lifecyclestage_evangelist_date",
    "hs_lifecyclestage_customer_date",
    "hs_lifecyclestage_subscriber_date",
    "hs_lifecyclestage_other_date",
]

_CONTACT_PROPERTIES_BOOLEAN = [
    "hs_content_membership_email_confirmed",
    "hs_created_by_conversations",
    "hs_data_privacy_ads_consent",
    "hs_email_bad_address",
    "hs_email_quarantined",
    "hs_facebook_ad_clicked",
    "hs_feedback_show_nps_web_survey",
    "hs_is_contact",
    "hs_is_unworked",
    "hs_read_only",
    "hs_sequences_is_enrolled",
    "hs_was_imported",
    "hs_email_optout",
    "hs_email_is_ineligible",
]


@dataclass
class HubSpotPropertyValue:
    """A property value from HubSpot.

    as_* methods can raise a ValueError if the value cannot be safely converted to the appropriate type.

    Empty strings are treated as 0 for int and float conversions.

    `value` property can only be a string, int, float, or datetime.
    """

    value: Any

    # Define dunder conversion methods in case casting is used.
    def __str__(self) -> str:
        return self.as_str()

    def __int__(self) -> int:
        return self.as_int()

    def __float__(self) -> float:
        return self.as_float()

    def as_datetime(self) -> datetime:
        match self.value:
            case datetime():
                return self.value
            case str():
                return datetime.fromisoformat(self.value)
            case _:
                raise

    def as_str(self) -> str:
        match self.value:
            case str():
                return self.value
            case _:
                # int and float are also valid strings
                return str(self.value)

    def as_int(self) -> int:
        match self.value:
            case int():
                return self.value
            case float():
                return int(self.value)
            case str():
                try:
                    # Parse the string as an int
                    return int(self.value)
                except ValueError:
                    if self.value.strip() == "":
                        return 0
                    raise

    def as_float(self) -> float:
        match self.value:
            case float():
                return self.value
            case int():
                return float(self.value)
            case str():
                try:
                    # Parse the string as a float
                    return float(self.value)
                except ValueError:
                    if self.value.strip() == "":
                        return 0.0
                    raise


@dataclass
class HubSpotContact:
    """The `additional_properties` field stores any additional properties that are
    available in the HubSpot contact system that callers can ask for. If found, they
    will be found here.

    You MUST specify all the fields when constructing this object.
    """
    id: str
    first_name: str
    last_name: str
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


def hubspot_list_contacts(
    limit: int = 100, pagination_token: Optional[HubSpotPaginationToken] = None
) -> Tuple[Sequence[HubSpotContact], Optional[HubSpotPaginationToken]]:
    """
    Fetch the list of contacts from HubSpot.

    No additional properties are returned from this API. Use search_contacts to retrieve
    specific contacts with additional properties.

    Args:
        limit: The maximum number of results to display per page. The maximum value for this is 100.
        pagination_token: Cursor for pagination.

    Returns:
        A tuple of a list of HubSpotContact objects and the next 'pagination_token' cursor, if
            available. If the next 'pagination_token' cursor is None, there is no more data to get.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    params = {}
    if limit:
        params["limit"] = limit
    if pagination_token:
        params["after"] = pagination_token.token

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    contacts = []
    for item in data["results"]:
        properties = item["properties"]
        contact = HubSpotContact(
            id=item["id"],
            created_at=datetime.fromisoformat(item["createdAt"]),
            updated_at=datetime.fromisoformat(item["updatedAt"]),
            archived=item["archived"],
            first_name=properties["firstname"],
            last_name=properties["lastname"],
            email=properties.get("email", ""),
            hs_object_id=properties["hs_object_id"],
            last_modified_date=datetime.fromisoformat(properties["lastmodifieddate"]),
            additional_properties={},
        )
        contacts.append(contact)

    next_pagination_token = (
        HubSpotPaginationToken(token=data["paging"]["next"]["after"])
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return contacts, next_pagination_token


def _coerce_properties_to_lutra(
    properties: Dict[str, Union[str, int, float, datetime, bool]],
    string_property_names: List[str],
    number_property_names: List[str],
    datetime_property_names: List[str],
    boolean_property_names: List[str],
) -> Dict[str, HubSpotPropertyValue]:
    coerced_properties: Dict[str, HubSpotPropertyValue] = {}
    for name, value in properties.items():

        if name in string_property_names:
            c_value = str(value)
        elif name in number_property_names:
            if isinstance(value, str):
                if "." in value:
                    c_value = float(value)
                else:
                    c_value = int(value)
            elif isinstance(value, int | float):
                c_value = value
            else:
                c_value = float(value)
        elif name in datetime_property_names:
            if isinstance(value, datetime):
                c_value = value
            elif isinstance(value, str):
                c_value = datetime.fromisoformat(value)
            else:
                raise ValueError(f"Unexpected datetime format: {value} ({type(value)})")
        elif name in boolean_property_names:
            c_value = bool(util.strtobool(str(value)))
        else:
            # Custom property, assume value is of right type.
            # TODO: Accept custom property schema and coerce accordingly.
            c_value = value

        coerced_properties[name] = HubSpotPropertyValue(value=c_value)

    return coerced_properties


def _coerce_properties_to_hubspot(
    properties: Dict[str, Union[str, int, float, datetime, bool, HubSpotPropertyValue]],
    string_property_names: List[str],
    number_property_names: List[str],
    datetime_property_names: List[str],
    boolean_property_names: List[str],
) -> Dict[str, Union[str, int, bool]]:
    coerced_properties = {}
    for name, value in properties.items():
        if isinstance(value, HubSpotPropertyValue):
            value = value.value

        if name in string_property_names:
            coerced_properties[name] = str(value)
        elif name in number_property_names:
            coerced_properties[name] = str(value)
        elif name in datetime_property_names:
            if isinstance(value, datetime):
                coerced_properties[name] = int(value.timestamp() * 1000)
            else:
                raise ValueError(f"Unexpected datetime format: {value} ({type(value)})")
        elif name in boolean_property_names:
            coerced_properties[name] = bool(util.strtobool(str(value)))
        else:
            # We don't support custom properties right now
            pass

    return coerced_properties

def hubspot_create_contacts(contacts: Sequence[HubSpotContact]) -> List[str]:
    """
    Create multiple contacts in HubSpot using the batch API.

    Args:
        contacts: A list of HubSpotContact objects to be created.

    Returns:
        A list of strings, where each string is the ID of a created contact.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"

    # Prepare the payload from the contacts list
    contacts_payload = []
    for contact in contacts:
        properties = {
            "firstname": contact.first_name,
            "lastname": contact.last_name,
            "email": contact.email
        }
        additional_properties = _coerce_properties_to_hubspot(
            contact.additional_properties,
            string_property_names=_CONTACT_PROPERTIES_STRING,
            number_property_names=_CONTACT_PROPERTIES_NUMBER,
            datetime_property_names=_CONTACT_PROPERTIES_DATETIME,
            boolean_property_names=_CONTACT_PROPERTIES_BOOLEAN,
        )
        properties.update(additional_properties)
        contact_data = {
            "properties": properties,
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


def hubspot_update_contacts(
    contact_updates: Dict[
        str,
        Sequence[
            Tuple[str, Union[str, int, float, datetime, bool, HubSpotPropertyValue]]
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
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"

    payload = [
        {
            "id": contact_id,
            "properties": _coerce_properties_to_hubspot(
                dict(properties),
                string_property_names=_CONTACT_PROPERTIES_STRING,
                number_property_names=_CONTACT_PROPERTIES_NUMBER,
                datetime_property_names=_CONTACT_PROPERTIES_DATETIME,
                boolean_property_names=_CONTACT_PROPERTIES_BOOLEAN,
            ),
        }
        for contact_id, properties in contact_updates.items()
    ]

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json={"inputs": payload})
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]

def _search_contacts(filters: List[Dict[str, str]], )

def hubspot_search_contacts(
    search_criteria: Dict[str, str],
    return_with_custom_properties: Sequence[str] = (),
) -> List[HubSpotContact]:
    """
    Search for HubSpot contacts based on various criteria.

    Default properties will always be fetched. However, properties with no values will not be in additional_properties
    dict. You MUST check whether the property exists in additional_properties before using it.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "firstname", "email") and values are the search values for those properties.
        return_with_custom_properties: A sequence of custom property names to fetch from found
            contacts. These will be included in additional_properties if they exist.

    Returns:
        Sequence[HubSpotContact]: A list of HubSpotContact objects matching the search
            criteria.
    """

    return_with_custom_properties = list(return_with_custom_properties)
    return_with_custom_properties += (
        _CONTACT_PROPERTIES_DATETIME
        + _CONTACT_PROPERTIES_BOOLEAN
        + _CONTACT_PROPERTIES_NUMBER
        + _CONTACT_PROPERTIES_STRING
    )
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"

    # Construct the filters based on the search criteria
    filters = []
    for property_name, value in search_criteria.items():
        if value:
            filters.append(
                {"propertyName": property_name, "operator": "EQ", "value": value}
            )
    if not filters:
        # We do this because if the search criteria values are just empty strings,
        # the call to the search API will fail with a 400 error.
        return []

    properties = ["firstname", "lastname", "email", "lastmodifieddate"]
    properties.extend(return_with_custom_properties)
    properties = list(set(properties))
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
        for property in return_with_custom_properties:
            val = property_values.get(property, None)
            if val:
                additional_property_values[property] = val

        additional_property_values = _coerce_properties_to_lutra(
            additional_property_values,
            string_property_names=_CONTACT_PROPERTIES_STRING,
            number_property_names=_CONTACT_PROPERTIES_NUMBER,
            datetime_property_names=_CONTACT_PROPERTIES_DATETIME,
            boolean_property_names=_CONTACT_PROPERTIES_BOOLEAN,
        )

        contact = HubSpotContact(
            id=item["id"],
            created_at=datetime.fromisoformat(
                item.get("createdAt", "1970-01-01T00:00:00Z")
            ),
            updated_at=datetime.fromisoformat(
                item.get("updatedAt", "1970-01-01T00:00:00Z")
            ),
            archived=item.get("archived", False),
            first_name=property_values.get("firstname", ""),
            last_name=property_values.get("lastname", ""),
            email=property_values.get("email", ""),
            hs_object_id=item["id"],
            last_modified_date=datetime.fromisoformat(
                property_values.get("lastmodifieddate", "1970-01-01T00:00:00Z")
            ),
            additional_properties=additional_property_values,
        )
        contacts.append(contact)

    return contacts


_COMPANY_PROPERTIES_STRING = [
    "about_us",
    "first_conversion_event_name",
    "founded_year",
    "hs_additional_domains",
    "hs_all_assigned_business_unit_ids",
    "hs_analytics_first_touch_converting_campaign",
    "hs_analytics_last_touch_converting_campaign",
    "hs_analytics_latest_source",
    "hs_analytics_latest_source_data_1",
    "hs_analytics_latest_source_data_2",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hs_annual_revenue_currency_code",
    "hs_avatar_filemanager_key",
    "hs_ideal_customer_profile",
    "hs_last_sales_activity_type",
    "hs_merged_object_ids",
    "hs_notes_next_activity_type",
    "hs_object_source",
    "hs_object_source_detail_1",
    "hs_object_source_detail_2",
    "hs_object_source_detail_3",
    "hs_object_source_id",
    "hs_object_source_label",
    "hs_pipeline",
    "hs_target_account",
    "hs_target_account_recommendation_state",
    "hs_unique_creation_key",
    "hs_user_ids_of_all_notification_followers",
    "hs_user_ids_of_all_notification_unfollowers",
    "hs_user_ids_of_all_owners",
    "recent_conversion_event_name",
    "timezone",
    "total_money_raised",
    "name",
    "owneremail",
    "twitterhandle",
    "ownername",
    "phone",
    "twitterbio",
    "address",
    "address2",
    "facebook_company_page",
    "city",
    "linkedin_company_page",
    "linkedinbio",
    "state",
    "googleplus_page",
    "engagements_last_meeting_booked_campaign",
    "engagements_last_meeting_booked_medium",
    "engagements_last_meeting_booked_source",
    "hubspot_owner_id",
    "zip",
    "country",
    "hubspot_team_id",
    "hs_all_owner_ids",
    "website",
    "domain",
    "hs_all_team_ids",
    "hs_all_accessible_team_ids",
    "industry",
    "lifecyclestage",
    "hs_lead_status",
    "type",
    "description",
    "web_technologies",
]

_COMPANY_PROPERTIES_NUMBER = [
    "facebookfans",
    "hs_analytics_num_page_views",
    "hs_analytics_num_visits",
    "hs_created_by_user_id",
    "hs_customer_success_ticket_sentiment",
    "hs_num_blockers",
    "hs_num_contacts_with_buying_roles",
    "hs_num_decision_makers",
    "hs_num_open_deals",
    "hs_object_id",
    "hs_object_source_user_id",
    "hs_pinned_engagement_id",
    "hs_predictivecontactscore_v2",
    "hs_source_object_id",
    "hs_target_account_probability",
    "hs_time_in_customer",
    "hs_time_in_evangelist",
    "hs_time_in_lead",
    "hs_time_in_marketingqualifiedlead",
    "hs_time_in_opportunity",
    "hs_time_in_other",
    "hs_time_in_salesqualifiedlead",
    "hs_time_in_subscriber",
    "hs_total_deal_value",
    "hs_updated_by_user_id",
    "num_associated_contacts",
    "num_associated_deals",
    "num_conversion_events",
    "recent_deal_amount",
    "total_revenue",
    "twitterfollowers",
    "num_contacted_notes",
    "num_notes",
    "numberofemployees",
    "annualrevenue",
    "hs_parent_company_id",
    "hs_num_child_companies",
    "hubspotscore",
    "days_to_close",
]

_COMPANY_PROPERTIES_DATETIME = [
    "first_conversion_date",
    "first_deal_created_date",
    "hs_analytics_first_timestamp",
    "hs_analytics_first_visit_timestamp",
    "hs_analytics_last_timestamp",
    "hs_analytics_last_visit_timestamp",
    "hs_analytics_latest_source_timestamp",
    "hs_createdate",
    "hs_date_entered_customer",
    "hs_date_entered_evangelist",
    "hs_date_entered_lead",
    "hs_date_entered_marketingqualifiedlead",
    "hs_date_entered_opportunity",
    "hs_date_entered_other",
    "hs_date_entered_salesqualifiedlead",
    "hs_date_entered_subscriber",
    "hs_date_exited_customer",
    "hs_date_exited_evangelist",
    "hs_date_exited_lead",
    "hs_date_exited_marketingqualifiedlead",
    "hs_date_exited_opportunity",
    "hs_date_exited_other",
    "hs_date_exited_salesqualifiedlead",
    "hs_date_exited_subscriber",
    "hs_last_booked_meeting_date",
    "hs_last_logged_call_date",
    "hs_last_open_task_date",
    "hs_last_sales_activity_date",
    "hs_last_sales_activity_timestamp",
    "hs_lastmodifieddate",
    "hs_latest_createdate_of_active_subscriptions",
    "hs_target_account_recommendation_snooze_time",
    "hubspot_owner_assigneddate",
    "recent_conversion_date",
    "recent_deal_close_date",
    "engagements_last_meeting_booked",
    "hs_latest_meeting_activity",
    "hs_sales_email_last_replied",
    "notes_last_contacted",
    "notes_last_updated",
    "notes_next_activity_date",
    "createdate",
    "closedate",
    "first_contact_createdate",
]

_COMPANY_PROPERTIES_BOOLEAN = [
    "hs_is_target_account",
    "hs_read_only",
    "hs_was_imported",
    "is_public",
]


@dataclass
class HubSpotCompany:
    """The `additional_properties` field stores any additional properties that are
    available in the HubSpot contact system that callers can ask for. If found, they
    will be found here.

    You MUST specify all the fields when constructing this object.
    """

    id: str
    name: str
    domain: Optional[str]
    hs_object_id: str
    last_modified_date: datetime
    additional_properties: Dict[str, HubSpotPropertyValue]
    created_at: datetime
    updated_at: datetime
    archived: bool


def hubspot_list_companies(
    limit: int = 100,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[Sequence[HubSpotCompany], Optional[str]]:
    """
    Fetch the list of companies from HubSpot.

    Args:
        limit: The maximum number of results to display per page.
        return_with_additional_properties: A sequence of property names to fetch from found
            companies. If present, the corresponding values will be provided in the
            HubSpotCompanyProperties field.
        pagination_token: Cursor for pagination.

    Returns:
        A tuple of a list of HubSpotCompany objects and the next 'pagination_token' cursor, if
            available. If the next 'pagination_token' cursor is None, there is no more data to get.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    properties = ["name", "domain", "hs_object_id", "hs_lastmodifieddate"]
    params = {}
    if limit:
        params["limit"] = limit
    if pagination_token:
        params["after"] = pagination_token.token
    params["properties"] = properties

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    companies = []
    for item in data["results"]:
        properties = item["properties"]
        company = HubSpotCompany(
            id=item["id"],
            created_at=datetime.fromisoformat(item["createdAt"]),
            updated_at=datetime.fromisoformat(item["updatedAt"]),
            archived=item["archived"],
            name=properties.get("name"),
            domain=properties.get("domain"),
            hs_object_id=properties.get("hs_object_id"),
            last_modified_date=datetime.fromisoformat(
                properties["hs_lastmodifieddate"]
            ),
            additional_properties={},
        )
        companies.append(company)
    next_pagination_token = (
        HubSpotPaginationToken(token=data["paging"]["next"]["after"])
        if "paging" in data and "next" in data["paging"]
        else None
    )

    next_pagination_token = (
        data["paging"]["next"]["after"]
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return companies, next_pagination_token


def hubspot_create_companies(companies: Sequence[HubSpotCompany]) -> List[str]:
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

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    # Extract and return the IDs of the created company
    return [result["id"] for result in data["results"]]


def hubspot_update_companies(
    company_updates: Dict[
        str,
        Sequence[
            Tuple[str, Union[str, int, float, datetime, bool, HubSpotPropertyValue]]
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
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/update"
    payload = [
        {
            "id": company_id,
            "properties": _coerce_properties_to_hubspot(
                dict(properties),
                string_property_names=_COMPANY_PROPERTIES_STRING,
                number_property_names=_COMPANY_PROPERTIES_NUMBER,
                datetime_property_names=_COMPANY_PROPERTIES_DATETIME,
                boolean_property_names=_COMPANY_PROPERTIES_BOOLEAN,
            ),
        }
        for company_id, properties in company_updates.items()
    ]
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json={"inputs": payload})
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]


def hubspot_search_companies(
    search_criteria: Dict[str, str],
    return_with_custom_properties: Sequence[str] = (),
) -> List[HubSpotCompany]:
    """
    Search for companies in HubSpot CRM based on various criteria.

    Default properties will always be fetched. However, properties with no values will not be in additional_properties
    dict. You MUST check whether the property exists in additional_properties before using it.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "name", "domain") and values are the search values for those properties.
        return_with_custom_properties: A sequence of custom property names to fetch from found
            contacts. These will be included in additional_properties if they exist.

    Returns:
        Sequence[HubSpotCompany]: A list of HubSpotCompany objects matching the search
            criteria.
    """
    return_with_custom_properties = list(return_with_custom_properties)
    return_with_custom_properties += (
        _COMPANY_PROPERTIES_DATETIME
        + _COMPANY_PROPERTIES_BOOLEAN
        + _COMPANY_PROPERTIES_NUMBER
        + _COMPANY_PROPERTIES_STRING
    )
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"

    # Construct the filters based on the search criteria
    filters = []
    for property_name, value in search_criteria.items():
        filters.append(
            {"propertyName": property_name, "operator": "EQ", "value": value}
        )
    if not filters:
        # We do this because if the search criteria values are just empty strings,
        # the call to the search API will fail with a 400 error.
        return []

    properties = [
        "name",
        "domain",
    ]
    properties.extend(return_with_custom_properties)
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
        for property in return_with_custom_properties:
            val = property_values.get(property, None)
            if val:
                additional_property_values[property] = val
        additional_property_values = _coerce_properties_to_lutra(
            additional_property_values,
            string_property_names=_COMPANY_PROPERTIES_STRING,
            number_property_names=_COMPANY_PROPERTIES_NUMBER,
            datetime_property_names=_COMPANY_PROPERTIES_DATETIME,
            boolean_property_names=_COMPANY_PROPERTIES_BOOLEAN,
        )

        company = HubSpotCompany(
            id=item["id"],
            created_at=datetime.fromisoformat(
                item.get("createdAt", "1970-01-01T00:00:00Z")
            ),
            updated_at=datetime.fromisoformat(
                item.get("updatedAt", "1970-01-01T00:00:00Z")
            ),
            archived=item.get("archived", False),
            name=property_values.get("name", ""),
            domain=property_values.get("domain", ""),
            hs_object_id=item["id"],
            last_modified_date=datetime.fromisoformat(
                property_values.get("lastmodifieddate", "1970-01-01T00:00:00Z")
            ),
            additional_properties=additional_property_values,
        )
        companies.append(company)

    return companies


_DEAL_PROPERTIES_STRING = [
    "deal_currency_code",
    "hs_all_assigned_business_unit_ids",
    "hs_all_collaborator_owner_ids",
    "hs_all_deal_split_owner_ids",
    "hs_analytics_latest_source",
    "hs_analytics_latest_source_company",
    "hs_analytics_latest_source_contact",
    "hs_analytics_latest_source_data_1",
    "hs_analytics_latest_source_data_1_company",
    "hs_analytics_latest_source_data_1_contact",
    "hs_analytics_latest_source_data_2",
    "hs_analytics_latest_source_data_2_company",
    "hs_analytics_latest_source_data_2_contact",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hs_campaign",
    "hs_deal_amount_calculation_preference",
    "hs_latest_approval_status",
    "hs_line_item_global_term_hs_discount_percentage",
    "hs_line_item_global_term_hs_recurring_billing_period",
    "hs_line_item_global_term_hs_recurring_billing_start_date",
    "hs_line_item_global_term_recurringbillingfrequency",
    "hs_manual_forecast_category",
    "hs_merged_object_ids",
    "hs_next_step",
    "hs_notes_next_activity_type",
    "hs_object_source",
    "hs_object_source_detail_1",
    "hs_object_source_detail_2",
    "hs_object_source_detail_3",
    "hs_object_source_id",
    "hs_object_source_label",
    "hs_priority",
    "hs_tag_ids",
    "hs_unique_creation_key",
    "hs_user_ids_of_all_notification_followers",
    "hs_user_ids_of_all_notification_unfollowers",
    "hs_user_ids_of_all_owners",
    "dealname",
    "dealstage",
    "pipeline",
    "engagements_last_meeting_booked_campaign",
    "engagements_last_meeting_booked_medium",
    "engagements_last_meeting_booked_source",
    "hubspot_owner_id",
    "hubspot_team_id",
    "dealtype",
    "hs_all_owner_ids",
    "description",
    "hs_all_team_ids",
    "hs_all_accessible_team_ids",
    "closed_lost_reason",
    "closed_won_reason",
]

_DEAL_PROPERTIES_NUMBER = [
    "amount_in_home_currency",
    "days_to_close",
    "hs_acv",
    "hs_arr",
    "hs_closed_amount",
    "hs_closed_amount_in_home_currency",
    "hs_closed_won_count",
    "hs_created_by_user_id",
    "hs_days_to_close_raw",
    "hs_deal_score",
    "hs_deal_stage_probability",
    "hs_deal_stage_probability_shadow",
    "hs_exchange_rate",
    "hs_forecast_amount",
    "hs_forecast_probability",
    "hs_is_open_count",
    "hs_latest_approval_status_approval_id",
    "hs_likelihood_to_close",
    "hs_mrr",
    "hs_num_associated_active_deal_registrations",
    "hs_num_associated_deal_registrations",
    "hs_num_associated_deal_splits",
    "hs_num_of_associated_line_items",
    "hs_num_target_accounts",
    "hs_object_id",
    "hs_object_source_user_id",
    "hs_pinned_engagement_id",
    "hs_predicted_amount",
    "hs_predicted_amount_in_home_currency",
    "hs_projected_amount",
    "hs_projected_amount_in_home_currency",
    "hs_source_object_id",
    "hs_tcv",
    "hs_time_in_appointmentscheduled",
    "hs_time_in_closedlost",
    "hs_time_in_closedwon",
    "hs_time_in_contractsent",
    "hs_time_in_decisionmakerboughtin",
    "hs_time_in_presentationscheduled",
    "hs_time_in_qualifiedtobuy",
    "hs_updated_by_user_id",
    "hs_v2_cumulative_time_in_appointmentscheduled",
    "hs_v2_cumulative_time_in_closedlost",
    "hs_v2_cumulative_time_in_closedwon",
    "hs_v2_cumulative_time_in_contractsent",
    "hs_v2_cumulative_time_in_decisionmakerboughtin",
    "hs_v2_cumulative_time_in_presentationscheduled",
    "hs_v2_cumulative_time_in_qualifiedtobuy",
    "hs_v2_latest_time_in_appointmentscheduled",
    "hs_v2_latest_time_in_closedlost",
    "hs_v2_latest_time_in_closedwon",
    "hs_v2_latest_time_in_contractsent",
    "hs_v2_latest_time_in_decisionmakerboughtin",
    "hs_v2_latest_time_in_presentationscheduled",
    "hs_v2_latest_time_in_qualifiedtobuy",
    "amount",
    "num_contacted_notes",
    "num_notes",
    "num_associated_contacts",
]

_DEAL_PROPERTIES_DATETIME = [
    "hs_analytics_latest_source_timestamp",
    "hs_analytics_latest_source_timestamp_company",
    "hs_analytics_latest_source_timestamp_contact",
    "hs_closed_won_date",
    "hs_date_entered_appointmentscheduled",
    "hs_date_entered_closedlost",
    "hs_date_entered_closedwon",
    "hs_date_entered_contractsent",
    "hs_date_entered_decisionmakerboughtin",
    "hs_date_entered_presentationscheduled",
    "hs_date_entered_qualifiedtobuy",
    "hs_date_exited_appointmentscheduled",
    "hs_date_exited_closedlost",
    "hs_date_exited_closedwon",
    "hs_date_exited_contractsent",
    "hs_date_exited_decisionmakerboughtin",
    "hs_date_exited_presentationscheduled",
    "hs_date_exited_qualifiedtobuy",
    "hs_lastmodifieddate",
    "hs_v2_date_entered_appointmentscheduled",
    "hs_v2_date_entered_closedlost",
    "hs_v2_date_entered_closedwon",
    "hs_v2_date_entered_contractsent",
    "hs_v2_date_entered_decisionmakerboughtin",
    "hs_v2_date_entered_presentationscheduled",
    "hs_v2_date_entered_qualifiedtobuy",
    "hs_v2_date_exited_appointmentscheduled",
    "hs_v2_date_exited_closedlost",
    "hs_v2_date_exited_closedwon",
    "hs_v2_date_exited_contractsent",
    "hs_v2_date_exited_decisionmakerboughtin",
    "hs_v2_date_exited_presentationscheduled",
    "hs_v2_date_exited_qualifiedtobuy",
    "hubspot_owner_assigneddate",
    "closedate",
    "createdate",
    "engagements_last_meeting_booked",
    "hs_latest_meeting_activity",
    "hs_sales_email_last_replied",
    "notes_last_contacted",
    "notes_last_updated",
    "notes_next_activity_date",
    "hs_createdate",
]

_DEAL_PROPERTIES_BOOLEAN = [
    "hs_is_active_shared_deal",
    "hs_is_closed",
    "hs_is_closed_won",
    "hs_is_deal_split",
    "hs_line_item_global_term_hs_discount_percentage_enabled",
    "hs_line_item_global_term_hs_recurring_billing_period_enabled",
    "hs_line_item_global_term_hs_recurring_billing_start_date_enabled",
    "hs_line_item_global_term_recurringbillingfrequency_enabled",
    "hs_read_only",
    "hs_was_imported",
]


@dataclass
class HubSpotDeal:
    """The `additional_properties` field stores any additional properties that are
    available in the HubSpot deal system that callers can ask for.
    """

    id: str
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


def hubspot_list_deals(
    limit: int = 100,
    pagination_token: Optional[HubSpotPaginationToken] = None,
) -> Tuple[Sequence[HubSpotDeal], Optional[str]]:
    """
    Fetch the list of deals from HubSpot.

    Args:
        limit: The maximum number of results to display per page.
        pagination_token: Cursor for pagination.

    Returns:
        A tuple of a list of HubSpotDeal objects and the next 'pagination_token' cursor, if
        available. If the next 'pagination_token' cursor is None, there is no more data to get.
    """
    url = "https://api.hubapi.com/crm/v3/objects/deals"
    properties = [
        "dealname",
        "dealstage",
        "closedate",
        "amount",
        "hs_object_id",
        "hs_lastmodifieddate",
    ]
    params = {"properties": properties}
    if limit:
        params["limit"] = limit
    if pagination_token:
        params["after"] = pagination_token.token

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    deals = []
    for item in data["results"]:
        properties = item["properties"]
        deal = HubSpotDeal(
            id=item["id"],
            created_at=datetime.fromisoformat(item["createdAt"]),
            updated_at=datetime.fromisoformat(item["updatedAt"]),
            archived=item["archived"],
            dealname=properties.get("dealname"),
            dealstage=properties.get("dealstage"),
            closedate=(
                datetime.fromisoformat(properties["closedate"])
                if "closeddate" in properties
                else None
            ),
            amount=float(properties.get("amount", 0)),
            hs_object_id=properties.get("hs_object_id"),
            last_modified_date=datetime.fromisoformat(
                properties["hs_lastmodifieddate"]
            ),
            additional_properties={},
        )
        deals.append(deal)
    next_pagination_token = (
        data["paging"]["next"]["after"]
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return deals, next_pagination_token


def hubspot_create_deals(deals: Sequence[HubSpotDeal]) -> List[str]:
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

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    return [result["id"] for result in data["results"]]


def hubspot_update_deals(
    deal_updates: Dict[
        str,
        Sequence[
            Tuple[str, Union[str, int, float, datetime, bool, HubSpotPropertyValue]]
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
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch/update"
    payload = [
        {
            "id": deal_id,
            "properties": _coerce_properties_to_hubspot(
                dict(properties),
                string_property_names=_DEAL_PROPERTIES_STRING,
                number_property_names=_DEAL_PROPERTIES_NUMBER,
                datetime_property_names=_DEAL_PROPERTIES_DATETIME,
                boolean_property_names=_DEAL_PROPERTIES_BOOLEAN,
            ),
        }
        for deal_id, properties in deal_updates.items()
    ]
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json={"inputs": payload})
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]


def hubspot_search_deals(
    search_criteria: Dict[str, str],
    return_with_custom_properties: Sequence[str] = (),
) -> List[HubSpotDeal]:
    """
    Search for HubSpot deals based on various criteria.

    Default properties will always be fetched. However, properties with no values will not be in the additional_properties
    dict. You MUST check whether the property exists in additional_properties before using it.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "dealname", "amount") and values are the search values for those properties.
        return_with_custom_properties: A sequence of custom property names to fetch from found
            deals. These will be included in additional_properties if they exist.

    Returns:
        Sequence[HubSpotDeal]: A list of HubSpotDeal objects matching the search
            criteria.
    """

    return_with_custom_properties = list(return_with_custom_properties)
    return_with_custom_properties += (
        _DEAL_PROPERTIES_DATETIME
        + _DEAL_PROPERTIES_BOOLEAN
        + _DEAL_PROPERTIES_NUMBER
        + _DEAL_PROPERTIES_STRING
    )
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"

    # Construct the filters based on the search criteria
    filters = []
    for property_name, value in search_criteria.items():
        filters.append(
            {"propertyName": property_name, "operator": "EQ", "value": value}
        )
    if not filters:
        # We do this because if the search criteria values are just empty strings,
        # the call to the search API will fail with a 400 error.
        return []

    properties = ["dealname", "dealstage", "closedate", "amount", "lastmodifieddate"]
    properties.extend(return_with_custom_properties)
    payload = {"filterGroups": [{"filters": filters}], "properties": properties}

    deals = []
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    for item in data.get("results", []):
        property_values = item.get("properties", {})
        additional_property_values = {}
        for property in return_with_custom_properties:
            val = property_values.get(property, None)
            if val:
                additional_property_values[property] = val

        additional_property_values = _coerce_properties_to_lutra(
            additional_property_values,
            string_property_names=_DEAL_PROPERTIES_STRING,
            number_property_names=_DEAL_PROPERTIES_NUMBER,
            datetime_property_names=_DEAL_PROPERTIES_DATETIME,
            boolean_property_names=_DEAL_PROPERTIES_BOOLEAN,
        )

        deal = HubSpotDeal(
            id=item["id"],
            created_at=datetime.fromisoformat(
                item.get("createdAt", "1970-01-01T00:00:00Z")
            ),
            updated_at=datetime.fromisoformat(
                item.get("updatedAt", "1970-01-01T00:00:00Z")
            ),
            archived=item.get("archived", False),
            dealname=property_values.get("dealname", ""),
            dealstage=property_values.get("dealstage", ""),
            closedate=(
                datetime.fromisoformat(
                    property_values.get("closedate", "1970-01-01T00:00:00Z")
                )
                if property_values.get("closedate")
                else None
            ),
            amount=float(property_values.get("amount", 0)),
            hs_object_id=item["id"],
            last_modified_date=datetime.fromisoformat(
                property_values.get("lastmodifieddate", "1970-01-01T00:00:00Z")
            ),
            additional_properties=additional_property_values,
        )
        deals.append(deal)

    return deals


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


def hubspot_fetch_associated_object_ids(
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


def hubspot_create_association_between_object_ids(
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
                }
            }
        ]
    }

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.post(url, json=params)
        response.raise_for_status()


def _merge_objects(url: str, primary_object_id: str, object_to_merge_id: str):
    params = {
        "objectIdToMerge": object_to_merge_id,
        "primaryObjectId": primary_object_id,
    }
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.post(url, json=params)
        response.raise_for_status()


def hubspot_merge_contacts(
    primary_contact: HubSpotContact, contact_to_merge: HubSpotContact
):
    ''' Merge contact_to_merge with primary_contact, retaining primary_contact
    '''
    url = "https://api.hubapi.com/crm/v3/objects/contacts/merge"
    _merge_objects(url, primary_contact.id, contact_to_merge.id)


def hubspot_merge_companies(
    primary_company: HubSpotCompany, company_to_merge: HubSpotCompany
):
    ''' Merge company_to_merge with primary_company, retaining primary_company
    '''
    url = "https://api.hubapi.com/crm/v3/objects/companies/merge"
    _merge_objects(url, primary_company.id, company_to_merge.id)
