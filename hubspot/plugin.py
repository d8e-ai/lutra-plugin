import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Sequence, Tuple, Union, Literal, List

import httpx

from lutraai.augmented_request_client import AugmentedTransport


def _handle_update_properties_response(response: httpx.Response):
    if response.status_code == 400:
        data = response.json()
        validation_results = data.get("validationResults")
        error_messages = []
        for result in validation_results:
            err_msg = result.get("localizedErrorMessage")
            if "not one of the allowed options:" in err_msg:  # enumeration
                matches = re.findall(r'value: "([^"]+)"', err_msg)
                matches_str = ", ".join(matches)
                error_messages.append(
                    f"Property '{result.get('name')}' only accepts following values: {matches_str}"
                )
            elif "not a valid_number" in err_msg:  # number
                error_messages.append(
                    f"Property '{result.get('name')}' only accepts numbers."
                )
            elif "was not a valid long" in err_msg:  # datetime
                error_messages.append(
                    f"Property '{result.get('name')}' is not a valid posix timestamp in milliseconds."
                )
            else:  # bool doesn't error no matter what you set it to
                pass
            raise RuntimeError("\n".join(error_messages))
    else:
        response.raise_for_status()

    data = response.json()
    return [result["id"] for result in data["results"]]


@dataclass
class HubSpotPropertiesToFetch:
    default_str_names: List[str]
    default_number_names: List[str]
    default_datetime_names: List[str]
    default_bool_names: List[str]
    custom_str_names: List[str]
    custom_number_names: List[str]
    custom_datetime_names: List[str]
    custom_bool_names: List[str]


def _to_transport_list(properties: HubSpotPropertiesToFetch) -> List[str]:
    return (
        properties.default_str_names
        + properties.default_number_names
        + properties.default_datetime_names
        + properties.default_bool_names
        + properties.custom_str_names
        + properties.custom_number_names
        + properties.custom_datetime_names
        + properties.custom_bool_names
    )


@dataclass
class HubSpotProperties:
    default_str_properties: Dict[str, str] = {}
    default_number_properties: Dict[str, float] = {}
    default_datetime_properties: Dict[str, datetime] = {}
    default_bool_properties: Dict[str, bool] = {}
    custom_str_properties: Dict[str, str] = {}
    custom_number_properties: Dict[str, float] = {}
    custom_datetime_properties: Dict[str, datetime] = {}
    custom_bool_properties: Dict[str, bool] = {}

def _to_transport_dict(properties: HubSpotProperties):
    transport_dict = {}
    transport_dict.update(properties.default_str_properties)
    transport_dict.update(properties.custom_str_properties)
    transport_dict.update(properties.default_number_properties)
    transport_dict.update(properties.custom_number_properties)
    transport_dict.update(properties.default_bool_properties)
    transport_dict.update(properties.custom_bool_properties)
    transport_dict.update(
        {
            name: int(val.timestamp() * 1000)
            for name, val in properties.default_datetime_properties
        }
    )
    transport_dict.update(
        {
            name: int(val.timestamp() * 1000)
            for name, val in properties.custom_datetime_properties
        }
    )
    return transport_dict


@dataclass
class HubSpotContactPropertiesToFetch(HubSpotPropertiesToFetch):
    default_str_names: List[
        Literal[
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
            "hs_email_optout_88607926",
            "hs_email_optout_96642417",
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
    ]
    default_number_names: List[
        Literal[
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
    ]
    default_datetime_names: List[
        Literal[
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
    ]
    default_bool_names: List[
        Literal[
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
    ]


@dataclass
class HubSpotContactProperties(HubSpotProperties):
    default_str_properties: Dict[
        Literal[
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
            "hs_email_optout_88607926",
            "hs_email_optout_96642417",
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
        ],
        str,
    ] = {}
    default_number_properties: Dict[
        Literal[
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
        ],
        float,
    ] = {}
    default_datetime_properties: Dict[
        Literal[
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
        ],
        datetime,
    ] = {}
    default_bool_properties: Dict[
        Literal[
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
        ],
        bool,
    ] = {}

def _from_contact_transport_dict(
    data: dict, properties_to_fetch: HubSpotContactPropertiesToFetch
) -> HubSpotContactProperties:
    default_str_properties = {
        name: data[name] for name in properties_to_fetch.default_str_names
    }
    custom_str_properties = {
        name: data[name] for name in properties_to_fetch.custom_str_names
    }
    default_number_properties = {
        name: data[name] for name in properties_to_fetch.default_number_names
    }
    custom_number_properties = {
        name: data[name] for name in properties_to_fetch.custom_number_names
    }
    default_datetime_properties = {
        name: datetime.fromisoformat(data[name])
        for name in properties_to_fetch.default_datetime_names
    }
    custom_datetime_properties = {
        name: datetime.fromisoformat(data[name])
        for name in properties_to_fetch.custom_datetime_names
    }
    default_bool_properties = {
        name: data[name] for name in properties_to_fetch.default_bool_names
    }
    custom_bool_properties = {
        name: data[name] for name in properties_to_fetch.custom_bool_names
    }

    return HubSpotContactProperties(
        default_str_properties=default_str_properties,
        custom_str_properties=custom_str_properties,
        default_number_properties=default_number_properties,
        custom_number_properties=custom_number_properties,
        default_datetime_properties=default_datetime_properties,
        custom_datetime_properties=custom_datetime_properties,
        default_bool_properties=default_bool_properties,
        custom_bool_properties=custom_bool_properties,
    )


@dataclass
class HubSpotUpdateContactRequest:
    id: str
    properties: HubSpotContactProperties


@dataclass
class HubSpotContact:
    id: str
    properties: HubSpotContactProperties
    created_at: datetime
    updated_at: datetime
    archived: bool


def hubspot_search_contacts(
    search_criteria: Dict[str, str],
    properties_to_fetch: HubSpotContactPropertiesToFetch,
) -> Sequence[HubSpotContact]:
    """Search for HubSpot contacts based on various criteria."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    filter = [
        {"propertyName": name, "operator": "EQ", "value": value}
        for name, value in search_criteria.items()
    ]

    properties = _to_transport_list(properties_to_fetch)

    payload = {
        "filterGroups": [{"filters": filter}],
        "properties": properties,
    }

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        contacts = []
        for contact_data in data.get("results", []):
            properties = _from_contact_transport_dict(
                contact_data["properties"], properties_to_fetch
            )
            contact = HubSpotContact(
                id=contact_data.get("id"),
                properties=properties,
                createdAt=datetime.fromisoformat(
                    contact_data.get("createdAt", "1970-01-01T00:00:00Z")
                ),
                updatedAt=datetime.fromisoformat(
                    contact_data.get("updatedAt", "1970-01-01T00:00:00Z")
                ),
                archived=contact_data.get("archived", False),
            )
            contacts.append(contact)
        return contacts


def hubspot_update_contacts(
    requests: List[HubSpotUpdateContactRequest],
) -> Sequence[str]:
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"
    payload = []
    for request in requests:
        payload.append(
            {"id": request.id, "properties": _to_transport_dict(request.properties)}
        )

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        return _handle_update_properties_response(response)


def hubspot_create_contacts(
    contacts: Sequence[HubSpotContactProperties],
) -> Sequence[str]:
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"
    inputs = []
    for contact_properties in contacts:
        inputs.append({"properties": _to_transport_dict(contact_properties)})
    payload = {"inputs": inputs}
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]


@dataclass
class HubSpotCompanyPropertiesToFetch(HubSpotPropertiesToFetch):
    default_str_names: List[
        Literal[
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
    ]
    default_number_names: List[
        Literal[
            "facebookfans",
            "hs_analytics_num_page_views",
            "hs_analytics_num_page_views_cardinality_sum_e46e85b0",
            "hs_analytics_num_visits",
            "hs_analytics_num_visits_cardinality_sum_53d952a6",
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
            "hs_predictivecontactscore_v2_next_max_max_d4e58c1e",
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
            "num_conversion_events_cardinality_sum_d095f14b",
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
    ]
    default_datetime_names: List[
        Literal[
            "closedate_timestamp_earliest_value_a2a17e6e",
            "first_contact_createdate_timestamp_earliest_value_78b50eea",
            "first_conversion_date",
            "first_conversion_date_timestamp_earliest_value_61f58f2c",
            "first_conversion_event_name_timestamp_earliest_value_68ddae0a",
            "first_deal_created_date",
            "hs_analytics_first_timestamp",
            "hs_analytics_first_timestamp_timestamp_earliest_value_11e3a63a",
            "hs_analytics_first_touch_converting_campaign_timestamp_earliest_value_4757fe10",
            "hs_analytics_first_visit_timestamp",
            "hs_analytics_first_visit_timestamp_timestamp_earliest_value_accc17ae",
            "hs_analytics_last_timestamp",
            "hs_analytics_last_timestamp_timestamp_latest_value_4e16365a",
            "hs_analytics_last_touch_converting_campaign_timestamp_latest_value_81a64e30",
            "hs_analytics_last_visit_timestamp",
            "hs_analytics_last_visit_timestamp_timestamp_latest_value_999a0fce",
            "hs_analytics_latest_source_timestamp",
            "hs_analytics_source_data_1_timestamp_earliest_value_9b2f1fa1",
            "hs_analytics_source_data_2_timestamp_earliest_value_9b2f9400",
            "hs_analytics_source_timestamp_earliest_value_25a3a52c",
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
            "recent_conversion_date_timestamp_latest_value_72856da1",
            "recent_conversion_event_name_timestamp_latest_value_66c820bf",
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
    ]
    default_bool_names: List[
        Literal["hs_is_target_account", "hs_read_only", "hs_was_imported", "is_public"]
    ]
    custom_str_names: List[str]
    custom_number_names: List[str]
    custom_datetime_names: List[str]
    custom_bool_names: List[str]


@dataclass
class HubSpotCompanyProperties(HubSpotProperties):
    default_str_properties: Dict[
        Literal[
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
        ],
        str,
    ] = {}
    default_number_properties: Dict[
        Literal[
            "facebookfans",
            "hs_analytics_num_page_views",
            "hs_analytics_num_page_views_cardinality_sum_e46e85b0",
            "hs_analytics_num_visits",
            "hs_analytics_num_visits_cardinality_sum_53d952a6",
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
            "hs_predictivecontactscore_v2_next_max_max_d4e58c1e",
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
            "num_conversion_events_cardinality_sum_d095f14b",
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
        ],
        float,
    ] = {}
    default_datetime_properties: Dict[
        Literal[
            "closedate_timestamp_earliest_value_a2a17e6e",
            "first_contact_createdate_timestamp_earliest_value_78b50eea",
            "first_conversion_date",
            "first_conversion_date_timestamp_earliest_value_61f58f2c",
            "first_conversion_event_name_timestamp_earliest_value_68ddae0a",
            "first_deal_created_date",
            "hs_analytics_first_timestamp",
            "hs_analytics_first_timestamp_timestamp_earliest_value_11e3a63a",
            "hs_analytics_first_touch_converting_campaign_timestamp_earliest_value_4757fe10",
            "hs_analytics_first_visit_timestamp",
            "hs_analytics_first_visit_timestamp_timestamp_earliest_value_accc17ae",
            "hs_analytics_last_timestamp",
            "hs_analytics_last_timestamp_timestamp_latest_value_4e16365a",
            "hs_analytics_last_touch_converting_campaign_timestamp_latest_value_81a64e30",
            "hs_analytics_last_visit_timestamp",
            "hs_analytics_last_visit_timestamp_timestamp_latest_value_999a0fce",
            "hs_analytics_latest_source_timestamp",
            "hs_analytics_source_data_1_timestamp_earliest_value_9b2f1fa1",
            "hs_analytics_source_data_2_timestamp_earliest_value_9b2f9400",
            "hs_analytics_source_timestamp_earliest_value_25a3a52c",
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
            "recent_conversion_date_timestamp_latest_value_72856da1",
            "recent_conversion_event_name_timestamp_latest_value_66c820bf",
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
        ],
        datetime,
    ] = {}
    default_bool_properties: Dict[
        Literal["hs_is_target_account", "hs_read_only", "hs_was_imported", "is_public"],
        bool,
    ] = {}

def _from_company_transport_dict(
    data: dict, properties_to_fetch: HubSpotCompanyPropertiesToFetch
) -> HubSpotCompanyProperties:
    default_str_properties = {
        name: data[name] for name in properties_to_fetch.default_str_names
    }
    custom_str_properties = {
        name: data[name] for name in properties_to_fetch.custom_str_names
    }
    default_number_properties = {
        name: data[name] for name in properties_to_fetch.default_number_names
    }
    custom_number_properties = {
        name: data[name] for name in properties_to_fetch.custom_number_names
    }
    default_datetime_properties = {
        name: datetime.fromisoformat(data[name])
        for name in properties_to_fetch.default_datetime_names
    }
    custom_datetime_properties = {
        name: datetime.fromisoformat(data[name])
        for name in properties_to_fetch.custom_datetime_names
    }
    default_bool_properties = {
        name: data[name] for name in properties_to_fetch.default_bool_names
    }
    custom_bool_properties = {
        name: data[name] for name in properties_to_fetch.custom_bool_names
    }

    return HubSpotCompanyProperties(
        default_str_properties=default_str_properties,
        custom_str_properties=custom_str_properties,
        default_number_properties=default_number_properties,
        custom_number_properties=custom_number_properties,
        default_datetime_properties=default_datetime_properties,
        custom_datetime_properties=custom_datetime_properties,
        default_bool_properties=default_bool_properties,
        custom_bool_properties=custom_bool_properties,
    )


@dataclass
class HubSpotUpdateCompanyRequest:
    id: str
    properties: HubSpotCompanyProperties


@dataclass
class HubSpotCompany:
    id: str
    properties: HubSpotCompanyProperties
    created_at: datetime
    updated_at: datetime
    archived: bool


def hubspot_search_companies(
    search_criteria: Dict[str, str],
    properties_to_fetch: HubSpotCompanyPropertiesToFetch,
) -> Sequence[HubSpotCompany]:
    """Search for HubSpot companies based on various criteria."""
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    filters = [
        {"propertyName": name, "operator": "EQ", "value": value}
        for name, value in search_criteria.items()
    ]

    properties = _to_transport_list(properties_to_fetch)

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": properties,
    }

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        companies = []
        for company_data in data.get("results", []):
            properties = _from_company_transport_dict(
                company_data["properties"], properties_to_fetch
            )
            company = HubSpotCompany(
                id=company_data.get("id"),
                properties=properties,
                created_at=datetime.fromisoformat(
                    company_data.get("createdAt", "1970-01-01T00:00:00Z")
                ),
                updated_at=datetime.fromisoformat(
                    company_data.get("updatedAt", "1970-01-01T00:00:00Z")
                ),
                archived=company_data.get("archived", False),
            )
            companies.append(company)
        return companies


def hubspot_update_companies(
    requests: List[HubSpotUpdateCompanyRequest],
) -> Sequence[str]:
    """Update multiple HubSpot companies in a single request."""
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/update"
    payload = []
    for request in requests:
        payload.append(
            {"id": request.id, "properties": _to_transport_dict(request.properties)}
        )

    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        return _handle_update_properties_response(response)


def hubspot_create_companies(
    companies: Sequence[HubSpotCompanyProperties],
) -> Sequence[str]:
    """Create multiple HubSpot companies in a single request."""
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/create"
    inputs = []
    for company_properties in companies:
        inputs.append({"properties": _to_transport_dict(company_properties)})
    payload = {"inputs": inputs}
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        return [result["id"] for result in data["results"]]


@dataclass
class HubSpotDealPropertiesToFetch(HubSpotPropertiesToFetch):
    default_str_names: List[
        Literal[
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
    ]
    default_number_names: List[
        Literal[
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
    ]
    default_datetime_names: List[
        Literal[
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
    ]
    default_bool_names: List[
        Literal[
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
    ]


@dataclass
class HubSpotDealProperties(HubSpotProperties):
    default_str_properties: Dict[
        Literal[
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
        ],
        str,
    ] = {}
    default_number_properties: Dict[
        Literal[
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
        ],
        float,
    ] = {}
    default_datetime_properties: Dict[
        Literal[
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
        ],
        datetime,
    ] = {}
    default_bool_properties: Dict[
        Literal[
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
        ],
        bool,
    ] = {}

def _from_deal_transport_dict(
    data: dict, properties_to_fetch: HubSpotDealPropertiesToFetch
) -> HubSpotDealProperties:
    default_str_properties = {
        name: data[name] for name in properties_to_fetch.default_str_names
    }
    custom_str_properties = {
        name: data[name] for name in properties_to_fetch.custom_str_names
    }
    default_number_properties = {
        name: data[name] for name in properties_to_fetch.default_number_names
    }
    custom_number_properties = {
        name: data[name] for name in properties_to_fetch.custom_number_names
    }
    default_datetime_properties = {
        name: datetime.fromisoformat(data[name])
        for name in properties_to_fetch.default_datetime_names
    }
    custom_datetime_properties = {
        name: datetime.fromisoformat(data[name])
        for name in properties_to_fetch.custom_datetime_names
    }
    default_bool_properties = {
        name: data[name] for name in properties_to_fetch.default_bool_names
    }
    custom_bool_properties = {
        name: data[name] for name in properties_to_fetch.custom_bool_names
    }

    return HubSpotDealProperties(
        default_str_properties=default_str_properties,
        custom_str_properties=custom_str_properties,
        default_number_properties=default_number_properties,
        custom_number_properties=custom_number_properties,
        default_datetime_properties=default_datetime_properties,
        custom_datetime_properties=custom_datetime_properties,
        default_bool_properties=default_bool_properties,
        custom_bool_properties=custom_bool_properties,
    )


@dataclass
class HubSpotUpdateDealRequest:
    id: str
    properties: HubSpotDealProperties


@dataclass
class HubSpotDeal:
    id: str
    properties: HubSpotDealProperties
    created_at: datetime
    updated_at: datetime
    archived: bool


def hubspot_fetch_deals(
    ids: Sequence[str], properties_to_fetch: HubSpotDealPropertiesToFetch
) -> Sequence[HubSpotDeal]:
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch/read"
    payload = {
        "inputs": [{"id": id} for id in ids],
        "properties": _to_transport_list(properties_to_fetch),
    }
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_hubspot),
    ) as client:
        response = client.post(url, json=payload)
        data = response.json()
        deals = []
        for deal_data in data.get("results", []):
            properties = _from_deal_transport_dict(
                deal_data["properties"], properties_to_fetch
            )
            deal = HubSpotDeal(
                id=deal_data.get("id"),
                properties=properties,
                created_at=datetime.fromisoformat(
                    deal_data.get("createdAt", "1970-01-01T00:00:00Z")
                ),
                updated_at=datetime.fromisoformat(
                    deal_data.get("updatedAt", "1970-01-01T00:00:00Z")
                ),
                archived=deal_data.get("archived", False),
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
