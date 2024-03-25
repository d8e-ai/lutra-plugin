from datetime import datetime
from dataclasses import dataclass
import httpx
from typing import Optional, List


_API_KEY = "INSERT YOUR API KEY HERE"


@dataclass
class EmploymentHistory:
    organization_name: Optional[str]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    title: Optional[str]


@dataclass
class FundingEvent:
    date: datetime
    news_url: Optional[str]
    type: str
    investors: List[str]
    amount: str
    currency: str


@dataclass
class Organization:
    name: str
    website_url: Optional[str]
    blog_url: Optional[str]
    angellist_url: Optional[str]
    twitter_url: Optional[str]
    facebook_url: Optional[str]
    phone: Optional[str]
    founding_year: Optional[int]
    industries: List[str]
    keywords: List[str]
    estimated_num_employees: Optional[int]
    street_address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    seo_description: Optional[str]
    annual_revenue: Optional[float]
    funding_events: List[FundingEvent]


@dataclass
class PeopleEnrichmentData:
    first_name: str
    last_name: str
    full_name: str
    linkedin_url: Optional[str]
    twitter_url: Optional[str]
    github_url: Optional[str]
    facebook_url: Optional[str]
    headline: Optional[str]
    email: Optional[str]
    state: Optional[str]
    city: Optional[str]
    country: Optional[str]
    employment_history: List[EmploymentHistory]
    organization: Organization
    departments: List[str]
    subdepartments: List[str]
    seniority: Optional[str]


def _parse_apollo_date(date_str: Optional[str]) -> Optional[datetime]:
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d")
    return None


def _parse_apollo_funding_event(event: dict) -> FundingEvent:
    return FundingEvent(
        date=datetime.fromisoformat(event["date"]),
        news_url=event.get("news_url"),
        type=event["type"],
        investors=event["investors"].split(", ") if event.get("investors") else [],
        amount=event["amount"],
        currency=event["currency"],
    )


def _parse_organization_data(org_data: dict) -> Organization:
    return Organization(
        name=org_data["name"],
        website_url=org_data.get("website_url"),
        blog_url=org_data.get("blog_url"),
        angellist_url=org_data.get("angellist_url"),
        twitter_url=org_data.get("twitter_url"),
        facebook_url=org_data.get("facebook_url"),
        phone=org_data.get("phone"),
        founding_year=(
            int(org_data["founded_year"]) if org_data.get("founded_year") else None
        ),
        industries=org_data.get("industries", []),
        keywords=org_data.get("keywords", []),
        estimated_num_employees=org_data.get("estimated_num_employees"),
        street_address=org_data.get("street_address"),
        city=org_data.get("city"),
        state=org_data.get("state"),
        country=org_data.get("country"),
        seo_description=org_data.get("seo_description"),
        annual_revenue=org_data.get("annual_revenue"),
        funding_events=[
            _parse_apollo_funding_event(event)
            for event in org_data.get("funding_events", [])
        ],
    )


def _parse_emploment(employment: dict) -> EmploymentHistory:
    return EmploymentHistory(
        organization_name=employment.get("organization_name"),
        start_date=_parse_apollo_date(employment.get("start_date")),
        end_date=_parse_apollo_date(employment.get("end_date")),
        title=employment.get("title"),
    )


def _parse_people_enrichment_data(enrichment_data: dict) -> PeopleEnrichmentData:
    employment_history = [
        _parse_emploment(employment)
        for employment in enrichment_data.get("employment_history", [])
    ]
    organization = _parse_organization_data(enrichment_data.get("organization", {}))

    return PeopleEnrichmentData(
        first_name=enrichment_data["first_name"],
        last_name=enrichment_data["last_name"],
        full_name=enrichment_data["name"],
        linkedin_url=enrichment_data.get("linkedin_url"),
        twitter_url=enrichment_data.get("twitter_url"),
        github_url=enrichment_data.get("github_url"),
        facebook_url=enrichment_data.get("facebook_url"),
        headline=enrichment_data.get("headline"),
        email=enrichment_data.get("email"),
        state=enrichment_data.get("state"),
        city=enrichment_data.get("city"),
        country=enrichment_data.get("country"),
        employment_history=employment_history,
        organization=organization,
        departments=enrichment_data.get("departments", []),
        subdepartments=enrichment_data.get("subdepartments", []),
        seniority=enrichment_data.get("seniority"),
    )


def fetch_people_enrichment_data(
    first_name: str, last_name: str, email: str
) -> PeopleEnrichmentData:
    """Fetch enrichment data for  detail using Apollo's API."""
    url = "https://api.apollo.io/v1/people/match"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
    }
    data = {
        "api_key": _API_KEY,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
    }

    with httpx.Client() as client:
        response = client.post(url, json=data, headers=headers)
        response.raise_for_status()
        result = response.json()
        return _parse_people_enrichment_data(result.get("person", {}))
