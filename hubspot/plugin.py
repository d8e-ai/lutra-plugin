from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Sequence, Tuple, Union, Literal, List

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


@dataclass
class HubSpotContactDefaultPropertyName:
    name: Literal[
        "adopter_category",
        "company_size",
        "date_of_birth",
        "days_to_close",
        "degree",
        "field_of_study",
        "first_conversion_date",
        "first_conversion_event_name",
        "first_deal_created_date",
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
        "hs_content_membership_email_confirmed",
        "hs_content_membership_follow_up_enqueued_at",
        "hs_content_membership_notes",
        "hs_content_membership_registered_at",
        "hs_content_membership_registration_domain_sent_to",
        "hs_content_membership_registration_email_sent_at",
        "hs_content_membership_status",
        "hs_conversations_visitor_email",
        "hs_count_is_unworked",
        "hs_count_is_worked",
        "hs_created_by_conversations",
        "hs_created_by_user_id",
        "hs_createdate",
        "hs_data_privacy_ads_consent",
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
        "hs_email_bad_address",
        "hs_email_customer_quarantined_reason",
        "hs_email_domain",
        "hs_email_hard_bounce_reason",
        "hs_email_hard_bounce_reason_enum",
        "hs_email_quarantined",
        "hs_email_quarantined_reason",
        "hs_email_recipient_fatigue_recovery_time",
        "hs_email_sends_since_last_engagement",
        "hs_emailconfirmationstatus",
        "hs_facebook_ad_clicked",
        "hs_facebook_click_id",
        "hs_facebookid",
        "hs_feedback_last_nps_follow_up",
        "hs_feedback_last_nps_rating",
        "hs_feedback_last_survey_date",
        "hs_feedback_show_nps_web_survey",
        "hs_first_engagement_object_id",
        "hs_first_outreach_date",
        "hs_first_subscription_create_date",
        "hs_google_click_id",
        "hs_googleplusid",
        "hs_has_active_subscription",
        "hs_ip_timezone",
        "hs_is_contact",
        "hs_is_unworked",
        "hs_journey_stage",
        "hs_last_sales_activity_date",
        "hs_last_sales_activity_timestamp",
        "hs_last_sales_activity_type",
        "hs_lastmodifieddate",
        "hs_latest_disqualified_lead_date",
        "hs_latest_open_lead_date",
        "hs_latest_qualified_lead_date",
        "hs_latest_sequence_ended_date",
        "hs_latest_sequence_enrolled",
        "hs_latest_sequence_enrolled_date",
        "hs_latest_sequence_finished_date",
        "hs_latest_sequence_unenrolled_date",
        "hs_latest_source_timestamp",
        "hs_latest_subscription_create_date",
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
        "hs_notes_next_activity",
        "hs_notes_next_activity_type",
        "hs_object_id",
        "hs_object_source",
        "hs_object_source_detail_1",
        "hs_object_source_detail_2",
        "hs_object_source_detail_3",
        "hs_object_source_id",
        "hs_object_source_label",
        "hs_object_source_user_id",
        "hs_pinned_engagement_id",
        "hs_pipeline",
        "hs_predictivecontactscore_v2",
        "hs_predictivescoringtier",
        "hs_read_only",
        "hs_sa_first_engagement_date",
        "hs_sa_first_engagement_descr",
        "hs_sa_first_engagement_object_type",
        "hs_sales_email_last_clicked",
        "hs_sales_email_last_opened",
        "hs_searchable_calculated_international_mobile_number",
        "hs_searchable_calculated_international_phone_number",
        "hs_searchable_calculated_mobile_number",
        "hs_searchable_calculated_phone_number",
        "hs_sequences_actively_enrolled_count",
        "hs_sequences_enrolled_count",
        "hs_sequences_is_enrolled",
        "hs_source_object_id",
        "hs_source_portal_id",
        "hs_testpurge",
        "hs_testrollback",
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
        "hs_timezone",
        "hs_twitterid",
        "hs_unique_creation_key",
        "hs_updated_by_user_id",
        "hs_user_ids_of_all_notification_followers",
        "hs_user_ids_of_all_notification_unfollowers",
        "hs_user_ids_of_all_owners",
        "hs_v2_cumulative_time_in_customer",
        "hs_v2_cumulative_time_in_evangelist",
        "hs_v2_cumulative_time_in_lead",
        "hs_v2_cumulative_time_in_marketingqualifiedlead",
        "hs_v2_cumulative_time_in_opportunity",
        "hs_v2_cumulative_time_in_other",
        "hs_v2_cumulative_time_in_salesqualifiedlead",
        "hs_v2_cumulative_time_in_subscriber",
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
        "hs_v2_latest_time_in_customer",
        "hs_v2_latest_time_in_evangelist",
        "hs_v2_latest_time_in_lead",
        "hs_v2_latest_time_in_marketingqualifiedlead",
        "hs_v2_latest_time_in_opportunity",
        "hs_v2_latest_time_in_other",
        "hs_v2_latest_time_in_salesqualifiedlead",
        "hs_v2_latest_time_in_subscriber",
        "hs_was_imported",
        "hs_whatsapp_phone_number",
        "hubspot_owner_assigneddate",
        "ip_city",
        "ip_country",
        "ip_country_code",
        "ip_latlon",
        "ip_state",
        "ip_state_code",
        "ip_zipcode",
        "job_function",
        "lastmodifieddate",
        "marital_status",
        "military_status",
        "num_conversion_events",
        "num_unique_conversion_events",
        "recent_conversion_date",
        "recent_conversion_event_name",
        "recent_deal_amount",
        "recent_deal_close_date",
        "relationship_status",
        "school",
        "seniority",
        "start_date",
        "total_revenue",
        "work_email",
        "firstname",
        "hs_analytics_first_url",
        "hs_email_delivered",
        "hs_email_optout_88607926",
        "hs_email_optout_96642417",
        "twitterhandle",
        "currentlyinworkflow",
        "followercount",
        "hs_analytics_last_url",
        "hs_email_open",
        "lastname",
        "hs_analytics_num_page_views",
        "hs_email_click",
        "salutation",
        "twitterprofilephoto",
        "email",
        "hs_analytics_num_visits",
        "hs_email_bounce",
        "hs_persona",
        "hs_social_last_engagement",
        "hs_analytics_num_event_completions",
        "hs_email_optout",
        "hs_social_twitter_clicks",
        "mobilephone",
        "phone",
        "fax",
        "hs_analytics_first_timestamp",
        "hs_email_last_email_name",
        "hs_email_last_send_date",
        "hs_social_facebook_clicks",
        "address",
        "engagements_last_meeting_booked",
        "engagements_last_meeting_booked_campaign",
        "engagements_last_meeting_booked_medium",
        "engagements_last_meeting_booked_source",
        "hs_analytics_first_visit_timestamp",
        "hs_email_last_open_date",
        "hs_latest_meeting_activity",
        "hs_sales_email_last_replied",
        "hs_social_linkedin_clicks",
        "hubspot_owner_id",
        "notes_last_contacted",
        "notes_last_updated",
        "notes_next_activity_date",
        "num_contacted_notes",
        "num_notes",
        "owneremail",
        "ownername",
        "surveymonkeyeventlastupdated",
        "webinareventlastupdated",
        "city",
        "hs_analytics_last_timestamp",
        "hs_email_last_click_date",
        "hs_social_google_plus_clicks",
        "hubspot_team_id",
        "linkedinbio",
        "twitterbio",
        "hs_all_owner_ids",
        "hs_analytics_last_visit_timestamp",
        "hs_email_first_send_date",
        "hs_social_num_broadcast_clicks",
        "state",
        "hs_all_team_ids",
        "hs_analytics_source",
        "hs_email_first_open_date",
        "hs_latest_source",
        "zip",
        "country",
        "hs_all_accessible_team_ids",
        "hs_analytics_source_data_1",
        "hs_email_first_click_date",
        "hs_latest_source_data_1",
        "linkedinconnections",
        "hs_analytics_source_data_2",
        "hs_email_is_ineligible",
        "hs_language",
        "hs_latest_source_data_2",
        "kloutscoregeneral",
        "hs_analytics_first_referrer",
        "hs_email_first_reply_date",
        "jobtitle",
        "photo",
        "hs_analytics_last_referrer",
        "hs_email_last_reply_date",
        "message",
        "closedate",
        "hs_analytics_average_page_views",
        "hs_email_replied",
        "hs_analytics_revenue",
        "hs_lifecyclestage_lead_date",
        "hs_lifecyclestage_marketingqualifiedlead_date",
        "hs_lifecyclestage_opportunity_date",
        "lifecyclestage",
        "hs_lifecyclestage_salesqualifiedlead_date",
        "createdate",
        "hs_lifecyclestage_evangelist_date",
        "hs_lifecyclestage_customer_date",
        "hubspotscore",
        "company",
        "hs_lifecyclestage_subscriber_date",
        "hs_lifecyclestage_other_date",
        "website",
        "numemployees",
        "annualrevenue",
        "industry",
        "hs_predictivecontactscorebucket",
        "hs_predictivecontactscore",
    ]


@dataclass
class HubSpotContactCustomPropertyName:
    name: str


def hubspot_list_contacts(
    limit: int = 100, pagination_token: Optional[str] = None
) -> Tuple[Sequence[HubSpotContact], Optional[str]]:
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
        params["after"] = pagination_token

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

    next_pagination_token = (
        data["paging"]["next"]["after"]
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return contacts, next_pagination_token


def hubspot_create_contacts(contacts: Sequence[HubSpotContact]) -> Sequence[str]:
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
class ContactDefaultPropertyUpdate:
    """The data required to update a contact's default properties."""

    contact_id: str
    updated_properties: List[Tuple[HubSpotContactDefaultPropertyName, str]]


@dataclass
class ContactCustomPropertyUpdate:
    """The data required to update a contact's custom properties."""

    contact_id: str
    updated_properties: List[Tuple[HubSpotContactCustomPropertyName, str]]


def hubspot_update_contacts(
    default_properties: Sequence[ContactDefaultPropertyUpdate],
    custom_properties: Sequence[ContactCustomPropertyUpdate],
) -> Sequence[str]:
    """
    Update multiple contacts in HubSpot.

    Returns:
        Contact IDs that have been updated.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"

    merged_properties = {}

    for property_update in default_properties + custom_properties:
        contact_properties = merged_properties.setdefault(
            property_update.contact_id, {}
        )
        contact_properties.update(
            {prop.name: value for prop, value in property_update.updated_properties}
        )

    payload = [
        {"id": contact_id, "properties": properties}
        for contact_id, properties in merged_properties.items()
    ]

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json={"inputs": payload})
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]


def hubspot_search_contacts(
    search_criteria: Dict[str, str],
    return_with_additional_properties: Optional[
        Sequence[
            Union[HubSpotContactDefaultPropertyName, HubSpotContactCustomPropertyName]
        ]
    ] = None,
) -> Sequence[HubSpotContact]:
    """
    Search for HubSpot contacts based on various criteria.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "firstname", "email") and values are the search values for those properties.
        return_with_additional_properties: A sequence of property names to fetch from found
            contacts. If present, the corresponding values will be provided in the
            HubSpotContactProperties return_with_additional_properties field. Standard hubspot
            properties are available, but users must know the names of custom properties
            if they are to be found.

    Returns:
        Sequence[HubSpotContact]: A list of HubSpotContact objects matching the search
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
    if return_with_additional_properties:
        properties.extend([prop.name for prop in return_with_additional_properties])
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
        if return_with_additional_properties:
            for property in return_with_additional_properties:
                val = property_values.get(property.name, None)
                if val:
                    additional_property_values[property.name] = val

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


@dataclass
class HubSpotCompanyDefaultPropertyName:
    name: Literal[
        "about_us",
        "facebookfans",
        "first_conversion_date",
        "first_conversion_event_name",
        "first_deal_created_date",
        "founded_year",
        "hs_additional_domains",
        "hs_all_assigned_business_unit_ids",
        "hs_analytics_first_timestamp",
        "hs_analytics_first_touch_converting_campaign",
        "hs_analytics_first_visit_timestamp",
        "hs_analytics_last_timestamp",
        "hs_analytics_last_touch_converting_campaign",
        "hs_analytics_last_visit_timestamp",
        "hs_analytics_latest_source",
        "hs_analytics_latest_source_data_1",
        "hs_analytics_latest_source_data_2",
        "hs_analytics_latest_source_timestamp",
        "hs_analytics_num_page_views",
        "hs_analytics_num_visits",
        "hs_analytics_source",
        "hs_analytics_source_data_1",
        "hs_analytics_source_data_2",
        "hs_annual_revenue_currency_code",
        "hs_avatar_filemanager_key",
        "hs_created_by_user_id",
        "hs_createdate",
        "hs_customer_success_ticket_sentiment",
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
        "hs_ideal_customer_profile",
        "hs_is_target_account",
        "hs_last_booked_meeting_date",
        "hs_last_logged_call_date",
        "hs_last_open_task_date",
        "hs_last_sales_activity_date",
        "hs_last_sales_activity_timestamp",
        "hs_last_sales_activity_type",
        "hs_lastmodifieddate",
        "hs_latest_createdate_of_active_subscriptions",
        "hs_merged_object_ids",
        "hs_notes_next_activity",
        "hs_notes_next_activity_type",
        "hs_num_blockers",
        "hs_num_contacts_with_buying_roles",
        "hs_num_decision_makers",
        "hs_num_open_deals",
        "hs_object_id",
        "hs_object_source",
        "hs_object_source_detail_1",
        "hs_object_source_detail_2",
        "hs_object_source_detail_3",
        "hs_object_source_id",
        "hs_object_source_label",
        "hs_object_source_user_id",
        "hs_pinned_engagement_id",
        "hs_pipeline",
        "hs_predictivecontactscore_v2",
        "hs_read_only",
        "hs_source_object_id",
        "hs_target_account",
        "hs_target_account_probability",
        "hs_target_account_recommendation_snooze_time",
        "hs_target_account_recommendation_state",
        "hs_time_in_customer",
        "hs_time_in_evangelist",
        "hs_time_in_lead",
        "hs_time_in_marketingqualifiedlead",
        "hs_time_in_opportunity",
        "hs_time_in_other",
        "hs_time_in_salesqualifiedlead",
        "hs_time_in_subscriber",
        "hs_total_deal_value",
        "hs_unique_creation_key",
        "hs_updated_by_user_id",
        "hs_user_ids_of_all_notification_followers",
        "hs_user_ids_of_all_notification_unfollowers",
        "hs_user_ids_of_all_owners",
        "hs_was_imported",
        "hubspot_owner_assigneddate",
        "is_public",
        "num_conversion_events",
        "recent_conversion_date",
        "recent_conversion_event_name",
        "recent_deal_amount",
        "recent_deal_close_date",
        "timezone",
        "total_money_raised",
        "total_revenue",
        "name",
        "owneremail",
        "twitterhandle",
        "ownername",
        "phone",
        "twitterbio",
        "twitterfollowers",
        "address",
        "address2",
        "facebook_company_page",
        "city",
        "linkedin_company_page",
        "linkedinbio",
        "state",
        "googleplus_page",
        "engagements_last_meeting_booked",
        "engagements_last_meeting_booked_campaign",
        "engagements_last_meeting_booked_medium",
        "engagements_last_meeting_booked_source",
        "hs_latest_meeting_activity",
        "hs_sales_email_last_replied",
        "hubspot_owner_id",
        "notes_last_contacted",
        "notes_last_updated",
        "notes_next_activity_date",
        "num_contacted_notes",
        "num_notes",
        "zip",
        "country",
        "hubspot_team_id",
        "hs_all_owner_ids",
        "website",
        "domain",
        "hs_all_team_ids",
        "hs_all_accessible_team_ids",
        "numberofemployees",
        "industry",
        "annualrevenue",
        "lifecyclestage",
        "hs_lead_status",
        "hs_parent_company_id",
        "type",
        "description",
        "hs_num_child_companies",
        "hubspotscore",
        "createdate",
        "closedate",
        "first_contact_createdate",
        "days_to_close",
        "web_technologies",
    ]


@dataclass
class HubSpotCompanyCustomPropertyName:
    name: str


def _parse_company(data: dict) -> HubSpotCompany:
    properties = data["properties"]

    company_properties = HubSpotCompanyProperties(
        name=properties["name"],
        domain=properties["domain"],
        hs_object_id=properties["hs_object_id"],
        last_modified_date=datetime.fromisoformat(properties["hs_lastmodifieddate"]),
        additional_properties={
            key: value
            for key, value in data["properties"].items()
            if key not in ["name", "domain", "hs_object_id", "hs_lastmodifieddate"]
            and value is not None
        },
    )

    return HubSpotCompany(
        id=data["id"],
        properties=company_properties,
        createdAt=datetime.fromisoformat(data["createdAt"]),
        updatedAt=datetime.fromisoformat(data["updatedAt"]),
        archived=data["archived"],
    )


def hubspot_list_companies(
    limit: int = 100,
    return_with_additional_properties: Optional[
        Sequence[
            Union[HubSpotCompanyDefaultPropertyName, HubSpotCompanyCustomPropertyName]
        ]
    ] = None,
    pagination_token: Optional[str] = None,
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
    if pagination_token:
        params["after"] = pagination_token
    if return_with_additional_properties:
        params["properties"].extend(
            [prop.name for prop in return_with_additional_properties]
        )

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    companies = []
    for item in data["results"]:
        company = _parse_company(item)
        companies.append(company)

    next_pagination_token = (
        data["paging"]["next"]["after"]
        if "paging" in data and "next" in data["paging"]
        else None
    )

    return companies, next_pagination_token


def hubspot_search_companies(
    search_criteria: Dict[str, str],
    return_with_additional_properties: Optional[
        Sequence[
            Union[HubSpotCompanyDefaultPropertyName, HubSpotCompanyCustomPropertyName]
        ]
    ] = None,
) -> Sequence[HubSpotCompany]:
    """
    Search for companies in HubSpot CRM based on various criteria.

    Args:
        search_criteria: A dictionary where keys are the property names (e.g.,
          "name", "domain") and values are the search values for those properties.
        return_with_additional_properties: A sequence of property names to fetch from found
            companies. If present, the corresponding values will be provided in the
            HubSpotCompanyProperties return_with_additional_properties field. Standard HubSpot
            properties are available, but users must know the names of custom properties
            if they are to be found.

    Returns:
        Sequence[HubSpotCompany]: A list of HubSpotCompany objects matching the search
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
    if return_with_additional_properties:
        properties.extend([prop.name for prop in return_with_additional_properties])

    payload = {"filterGroups": [{"filters": filters}], "properties": properties}

    companies = []
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

    for item in data.get("results", []):
        company = _parse_company(item)
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


def hubspot_fetch_deal_by_id(deal_id: str) -> HubSpotDeal:
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


def hubspot_fetch_company_by_id(
    company_id: str,
    return_with_additional_properties: Optional[
        Sequence[
            Union[HubSpotCompanyDefaultPropertyName, HubSpotCompanyCustomPropertyName]
        ]
    ] = None,
) -> HubSpotCompany:
    """
    Fetch a company from HubSpot CRM based on a company ID.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
    properties = [
        "name",
        "domain",
    ]
    if return_with_additional_properties:
        properties += [prop.name for prop in return_with_additional_properties]

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.get(url, params={"properties": ",".join(properties)})
        response.raise_for_status()
        data = response.json()
        return _parse_company(data)


@dataclass
class CompanyDefaultPropertyUpdate:
    """The data required to update a company's default properties."""

    company_id: str
    updated_properties: List[Tuple[HubSpotCompanyDefaultPropertyName, str]]


@dataclass
class CompanyCustomPropertyUpdate:
    """The data required to update a company's custom properties."""

    company_id: str
    updated_properties: List[Tuple[HubSpotCompanyCustomPropertyName, str]]


def hubspot_update_companies(
    default_properties: Sequence[CompanyDefaultPropertyUpdate],
    custom_properties: Sequence[CompanyCustomPropertyUpdate],
) -> Sequence[str]:
    """
    Update multiple companies in HubSpot.

    Returns:
        Company IDs that has been updated.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/update"

    merged_properties = {}

    for property_update in default_properties + custom_properties:
        company_properties = merged_properties.setdefault(
            property_update.company_id, {}
        )
        company_properties.update(
            {prop.name: value for prop, value in property_update.updated_properties}
        )

    payload = [
        {"id": company_id, "properties": properties}
        for company_id, properties in merged_properties.items()
    ]

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot)
    ) as client:
        response = client.post(url, json={"inputs": payload})
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]


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
    source_object_type: Union[HubSpotObjectType, HubSpotCustomObjectType],
    target_object_type: Union[HubSpotObjectType, HubSpotCustomObjectType],
    source_object_id: str,
) -> Sequence[str]:
    """
    Returns the IDs of target objects associated with the source object
    using the HubSpot association API. You must use this to find HubSpot
    objects that are associated to each other.
    """
    source_type_name = _HUBSPOT_OBJECT_TYPE_IDS.get(
        source_object_type.name, source_object_type.name
    )
    target_type_name = _HUBSPOT_OBJECT_TYPE_IDS.get(
        target_object_type.name, target_object_type.name
    )
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
