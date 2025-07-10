import hashlib
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from dateutil.parser import parse as dateutil_parse
from pydantic import TypeAdapter

from lutraai.decorator import purpose
from lutraai.dependencies import AuthenticatedAsyncClient
from lutraai.dependencies.authentication import (
    InternalAllowedURL,
    InternalAuthenticatedClientConfig,
    InternalAuthTransportConfig,
    InternalOAuthSpec,
    InternalRefreshTokenConfig,
    InternalUserInfoConfig,
)
from lutraai.requests import raise_error_text


def _parse_xero_date(date_value: Any) -> datetime:
    """Parse Microsoft JSON date format: /Date(timestamp+offset)/ - returns UTC timezone-aware datetime"""
    if not date_value:
        return None

    if isinstance(date_value, datetime):
        # If already timezone-aware, return as-is; if naive, assume UTC
        if date_value.tzinfo is None:
            return date_value.replace(tzinfo=timezone.utc)
        return date_value

    # Handle Microsoft JSON date format
    match = re.match(r"/Date\((\d+)(?:([+-]\d{4}))?\)/", str(date_value))
    if match:
        timestamp_ms = int(match.group(1))
        timestamp_s = timestamp_ms / 1000
        return datetime.fromtimestamp(timestamp_s, tz=timezone.utc)

    # Fallback to regular date parsing
    try:
        dt = datetime.strptime(str(date_value), "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        # Try ISO format
        try:
            dt = datetime.fromisoformat(str(date_value).rstrip("Z"))
            # If the parsed datetime is naive, assume UTC
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            raise ValueError(f"Unable to parse date: {date_value}")


@dataclass
class XeroAddress:
    address_type: Literal["POBOX", "STREET", "UNIT"]
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    address_line3: Optional[str] = None
    address_line4: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    attention_to: Optional[str] = None


@dataclass
class XeroPhone:
    phone_number: Optional[str] = None
    phone_type: Optional[str] = None
    phone_area_code: Optional[str] = None
    phone_country_code: Optional[str] = None


@dataclass
class XeroContact:
    contact_id: str
    name: str
    contact_number: Optional[str] = None
    account_number: Optional[str] = None
    contact_status: Optional[Literal["ACTIVE", "ARCHIVED", "DELETED"]] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email_address: Optional[str] = None
    bank_account_details: Optional[str] = None
    company_number: Optional[str] = None
    tax_number: Optional[str] = None
    accounts_receivable_tax_type: Optional[str] = None
    accounts_payable_tax_type: Optional[str] = None
    addresses: Optional[List[XeroAddress]] = None
    phones: Optional[List[XeroPhone]] = None
    is_supplier: Optional[bool] = None
    is_customer: Optional[bool] = None
    default_currency: Optional[str] = None
    updated_date_utc: Optional[datetime] = None


@dataclass
class XeroLineItem:
    """Line item on a Xero invoice.

    IMPORTANT: Before creating line items, use xero_get_account() to get user selected account.
    The account_code field must match an existing account code from your Xero organization.

    Workflow:
    1. Call xero_get_account() to get available accounts
    2. Choose appropriate account_code from the returned accounts
    3. Use that account_code when creating XeroLineItem objects
    """

    description: str
    quantity: float
    unit_amount: float
    line_amount: float
    account_code: Optional[str] = None
    item_code: Optional[str] = None
    tax_type: Optional[str] = None
    tax_amount: Optional[float] = None
    line_item_id: Optional[str] = None


# Note: Payments, Credit Notes, Prepayments, Overpayments are not supported at this time
@dataclass
class XeroInvoice:
    invoice_id: str
    type: Literal["ACCPAY", "ACCREC"]
    contact: XeroContact
    date: datetime
    due_date: datetime
    status: Literal["DRAFT", "SUBMITTED", "AUTHORISED", "DELETED", "VOIDED", "PAID"]
    line_amount_types: Literal["Exclusive", "Inclusive", "NoTax"]
    line_items: List[XeroLineItem]
    sub_total: float
    total_tax: float
    total: float
    total_discount: Optional[float] = None
    invoice_number: Optional[str] = None
    reference: Optional[str] = None
    currency_code: Optional[str] = None
    currency_rate: Optional[float] = None
    branding_theme_id: Optional[str] = None
    url: Optional[str] = None
    sent_to_contact: Optional[bool] = None
    expected_payment_date: Optional[datetime] = None
    planned_payment_date: Optional[datetime] = None
    has_attachments: Optional[bool] = None
    repeating_invoice_id: Optional[str] = None
    amount_due: Optional[float] = None
    amount_paid: Optional[float] = None
    cis_deduction: Optional[float] = None
    fully_paid_on_date: Optional[datetime] = None
    amount_credited: Optional[float] = None
    sales_tax_calculation_type_code: Optional[str] = None
    invoice_addresses: Optional[List[Dict[str, Any]]] = None
    updated_date_utc: Optional[datetime] = None


@dataclass
class XeroTenant:
    """Information about a Xero tenant."""

    tenant_name: str
    tenant_id: str
    connection_id: str


@dataclass
class XeroOrganisation:
    """Information about a Xero organisation."""

    organisation_id: str
    name: str
    short_code: str
    legal_name: Optional[str] = None
    pays_tax: Optional[bool] = None
    version: Optional[str] = None
    organisation_type: Optional[str] = None
    base_currency: Optional[str] = None
    country_code: Optional[str] = None
    is_demo_company: Optional[bool] = None
    organisation_status: Optional[str] = None
    registration_number: Optional[str] = None
    employer_identification_number: Optional[str] = None
    tax_number: Optional[str] = None
    financial_year_end_day: Optional[int] = None
    financial_year_end_month: Optional[int] = None
    sales_tax_basis: Optional[str] = None
    sales_tax_period: Optional[str] = None
    default_sales_tax: Optional[str] = None
    default_purchases_tax: Optional[str] = None
    period_lock_date: Optional[datetime] = None
    end_of_year_lock_date: Optional[datetime] = None
    created_date_utc: Optional[datetime] = None
    timezone: Optional[str] = None
    organisation_entity_type: Optional[str] = None
    edition: Optional[str] = None
    class_: Optional[str] = None  # 'class' is a reserved keyword
    line_of_business: Optional[str] = None




@dataclass
class XeroPaginationToken:
    """Pagination token for Xero API requests."""

    offset: int


# Configure OAuth client for Xero API
xero_client = AuthenticatedAsyncClient(
    InternalAuthenticatedClientConfig(
        action_name="authenticated_request_xero",
        allowed_urls=(
            InternalAllowedURL(
                scheme=b"https",
                domain_suffix=b"api.xero.com",
                add_auth=True,
            ),
        ),
        base_url=None,
        auth_spec=InternalOAuthSpec(
            auth_name="Xero",
            auth_group="Xero",
            auth_type="oauth2",
            access_token_url="https://identity.xero.com/connect/token",
            authorize_url="https://login.xero.com/identity/connect/authorize",
            api_base_url="https://api.xero.com",
            checks=["state", "pkce"],
            userinfo_endpoint="https://api.xero.com/connections",
            userinfo_config=InternalUserInfoConfig(
                auth_userinfo_type="basic",
            ),
            userinfo_from_id_token=True,
            auth_transport_config=InternalAuthTransportConfig(
                transport_type="header",
                param_name="Authorization",
                param_value_format="Bearer {api_key}",
            ),
            profile_id_field="email",
            scopes_spec={
                "accounting.transactions": "Read and write access to bank transactions, credit notes, invoices, manual journals, overpayments, prepayments, purchase orders, and quotes.",
                "accounting.contacts": "Read and write access to contacts and contact groups.",
                "accounting.journals.read": "Read general ledger",
                "offline_access": "Offline access",
                "accounting.reports.read": "Read reports",
                "accounting.settings": "Account configuration",
                "openid": "Open ID",
            },
            scope_separator=" ",
            jwks_uri="",  # None available
            prompt="consent",
            server_metadata_url="https://identity.xero.com/.well-known/openid-configuration",
            access_type="offline",
            logo="https://storage.googleapis.com/lutra-2407-public/d57d1f501344bcd670537713c949bf66cedf5cac401ee2b04505a39432348464.svg",
            header_auth={
                "Authorization": "Bearer {api_key}",
                "Accept": "application/json",
            },
            refresh_token_config=InternalRefreshTokenConfig(
                auth_refresh_type="form",
                body_fields={
                    "client_id": "{client_id}",
                    "client_secret": "{client_secret}",
                    "refresh_token": "{refresh_token}",
                    "grant_type": "refresh_token",
                },
            ),
        ),
    ),
    provider_id="cef71a99-d62d-41a1-85d4-eed1553d9de9",
)

_XC_TA = None
_XC_LI = None
_XJ_TA = None
_XBA_TA = None
_XBT_TA = None
_XA_TA = None
_XOURL_TA = None
_XO_TA = None


def _convert_address_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API address data from PascalCase to snake_case."""
    return {
        "address_type": data.get("AddressType"),
        "address_line1": data.get("AddressLine1"),
        "address_line2": data.get("AddressLine2"),
        "address_line3": data.get("AddressLine3"),
        "address_line4": data.get("AddressLine4"),
        "city": data.get("City"),
        "region": data.get("Region"),
        "postal_code": data.get("PostalCode"),
        "country": data.get("Country"),
        "attention_to": data.get("AttentionTo"),
    }


def _convert_phone_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API phone data from PascalCase to snake_case."""
    return {
        "phone_number": data.get("PhoneNumber"),
        "phone_type": data.get("PhoneType"),
        "phone_area_code": data.get("PhoneAreaCode"),
        "phone_country_code": data.get("PhoneCountryCode"),
    }


def _convert_bank_account_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API bank account data from PascalCase to snake_case."""
    return {
        "account_id": data.get("AccountID"),
        "name": data.get("Name"),
        "code": data.get("Code"),
    }


def _convert_bank_transfer_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API bank transfer data from PascalCase to snake_case."""
    return {
        "bank_transfer_id": data.get("BankTransferID"),
        "date": _parse_xero_date(data.get("Date")),
        "amount": float(data.get("Amount", 0.0)),
        "from_bank_account": _convert_bank_account_to_snake_case(
            data.get("FromBankAccount", {})
        ),
        "to_bank_account": _convert_bank_account_to_snake_case(
            data.get("ToBankAccount", {})
        ),
        "created_date_utc": _parse_xero_date(data.get("CreatedDateUTC")),
        "from_bank_transaction_id": data.get("FromBankTransactionID"),
        "to_bank_transaction_id": data.get("ToBankTransactionID"),
        "from_is_reconciled": data.get("FromIsReconciled") == "true"
        if data.get("FromIsReconciled")
        else None,
        "to_is_reconciled": data.get("ToIsReconciled") == "true"
        if data.get("ToIsReconciled")
        else None,
        "reference": data.get("Reference"),
        "currency_rate": float(data.get("CurrencyRate"))
        if data.get("CurrencyRate")
        else None,
        "has_attachments": data.get("HasAttachments"),
    }


def _convert_contact_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API contact data from PascalCase to snake_case."""
    converted = {
        "contact_id": data.get("ContactID"),
        "name": data.get("Name"),
        "contact_number": data.get("ContactNumber"),
        "account_number": data.get("AccountNumber"),
        "contact_status": data.get("ContactStatus"),
        "first_name": data.get("FirstName"),
        "last_name": data.get("LastName"),
        "email_address": data.get("EmailAddress"),
        "bank_account_details": data.get("BankAccountDetails"),
        "company_number": data.get("CompanyNumber"),
        "tax_number": data.get("TaxNumber"),
        "accounts_receivable_tax_type": data.get("AccountsReceivableTaxType"),
        "accounts_payable_tax_type": data.get("AccountsPayableTaxType"),
        "is_supplier": data.get("IsSupplier"),
        "is_customer": data.get("IsCustomer"),
        "default_currency": data.get("DefaultCurrency"),
        "updated_date_utc": _parse_xero_date(data.get("UpdatedDateUTC")),
    }

    # Handle addresses
    if data.get("Addresses"):
        converted["addresses"] = [
            _convert_address_to_snake_case(addr) for addr in data["Addresses"]
        ]
    else:
        converted["addresses"] = None

    # Handle phones
    if data.get("Phones"):
        converted["phones"] = [
            _convert_phone_to_snake_case(phone) for phone in data["Phones"]
        ]
    else:
        converted["phones"] = None

    return converted


def _convert_line_item_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API line item data from PascalCase to snake_case."""
    return {
        "description": data.get("Description"),
        "quantity": data.get("Quantity"),
        "unit_amount": data.get("UnitAmount"),
        "line_amount": data.get("LineAmount"),
        "account_code": data.get("AccountCode"),
        "item_code": data.get("ItemCode"),
        "tax_type": data.get("TaxType"),
        "tax_amount": data.get("TaxAmount"),
        "line_item_id": data.get("LineItemID"),
    }


def _convert_journal_line_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API journal line data from PascalCase to snake_case."""
    return {
        "journal_line_id": data.get("JournalLineID"),
        "account_id": data.get("AccountID"),
        "account_code": data.get("AccountCode"),
        "account_type": data.get("AccountType"),
        "account_name": data.get("AccountName"),
        "description": data.get("Description"),
        "net_amount": data.get("NetAmount"),
        "gross_amount": data.get("GrossAmount"),
        "tax_amount": data.get("TaxAmount"),
        "tax_type": data.get("TaxType"),
        "tax_name": data.get("TaxName"),
        "tracking_categories": data.get("TrackingCategories"),
    }


def _convert_journal_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API journal data from PascalCase to snake_case."""
    converted = {
        "journal_id": data.get("JournalID"),
        "journal_date": _parse_xero_date(data.get("JournalDate")),
        "journal_number": data.get("JournalNumber"),
        "created_date_utc": _parse_xero_date(data.get("CreatedDateUTC")),
        "reference": data.get("Reference"),
        "source_id": data.get("SourceID"),
        "source_type": data.get("SourceType"),
    }

    # Handle journal lines
    if data.get("JournalLines"):
        converted["journal_lines"] = [
            _convert_journal_line_to_snake_case(line) for line in data["JournalLines"]
        ]
    else:
        converted["journal_lines"] = []

    return converted


def _convert_account_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API account data from PascalCase to snake_case."""
    return {
        "account_id": data.get("AccountID"),
        "code": data.get("Code"),
        "name": data.get("Name"),
        "type": data.get("Type"),
        "tax_type": data.get("TaxType"),
        "status": data.get("Status"),
        "description": data.get("Description"),
        "bank_account_number": data.get("BankAccountNumber"),
        "bank_account_type": data.get("BankAccountType"),
        "currency_code": data.get("CurrencyCode"),
        "enable_payments_to_account": data.get("EnablePaymentsToAccount"),
        "show_in_expense_claims": data.get("ShowInExpenseClaims"),
        "class_": data.get("Class"),  # 'class' is a reserved keyword in Python
        "system_account": data.get("SystemAccount"),
        "reporting_code": data.get("ReportingCode"),
        "reporting_code_updated_utc": _parse_xero_date(
            data.get("ReportingCodeUpdatedUTC")
        ),
        "reporting_code_name": data.get("ReportingCodeName"),
        "has_attachments": data.get("HasAttachments"),
        "updated_date_utc": _parse_xero_date(data.get("UpdatedDateUTC")),
        "add_to_watchlist": data.get("AddToWatchlist"),
    }


def _convert_organisation_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Xero API organisation data from PascalCase to snake_case."""
    return {
        "organisation_id": data.get("OrganisationID"),
        "name": data.get("Name"),
        "short_code": data.get("ShortCode"),
        "legal_name": data.get("LegalName"),
        "pays_tax": data.get("PaysTax"),
        "version": data.get("Version"),
        "organisation_type": data.get("OrganisationType"),
        "base_currency": data.get("BaseCurrency"),
        "country_code": data.get("CountryCode"),
        "is_demo_company": data.get("IsDemoCompany"),
        "organisation_status": data.get("OrganisationStatus"),
        "registration_number": data.get("RegistrationNumber"),
        "employer_identification_number": data.get("EmployerIdentificationNumber"),
        "tax_number": data.get("TaxNumber"),
        "financial_year_end_day": data.get("FinancialYearEndDay"),
        "financial_year_end_month": data.get("FinancialYearEndMonth"),
        "sales_tax_basis": data.get("SalesTaxBasis"),
        "sales_tax_period": data.get("SalesTaxPeriod"),
        "default_sales_tax": data.get("DefaultSalesTax"),
        "default_purchases_tax": data.get("DefaultPurchasesTax"),
        "period_lock_date": _parse_xero_date(data.get("PeriodLockDate")),
        "end_of_year_lock_date": _parse_xero_date(data.get("EndOfYearLockDate")),
        "created_date_utc": _parse_xero_date(data.get("CreatedDateUTC")),
        "timezone": data.get("Timezone"),
        "organisation_entity_type": data.get("OrganisationEntityType"),
        "edition": data.get("Edition"),
        "class_": data.get("Class"),  # 'class' is a reserved keyword in Python
        "line_of_business": data.get("LineOfBusiness"),
    }




def _parse_balance_sheet_line_item(
    section_row: Dict[str, Any], period_labels: List[str]
) -> Dict[str, Any] | None:
    """Parse a section row into a balance sheet line item (recursive for nested structure)."""
    title = section_row.get("Title", "").strip()
    if not title:
        return None

    children = []
    period_totals = {}

    for row in section_row.get("Rows", []):
        if row.get("RowType") == "Row" and row.get("Cells"):
            cells = row["Cells"]
            if len(cells) >= 2:  # At least name + one period
                # Extract account ID from attributes if available
                account_id = None
                if cells[0].get("Attributes"):
                    for attr in cells[0]["Attributes"]:
                        if attr.get("Id") == "account":
                            account_id = attr.get("Value")
                            break

                # Extract period values (skip first cell which is the name)
                period_values = {}
                for i in range(1, len(cells)):
                    if i - 1 < len(
                        period_labels
                    ):  # Ensure we have a corresponding period label
                        period_label = period_labels[i - 1]
                        try:
                            value = float(cells[i].get("Value", "0"))
                            period_values[period_label] = value
                        except (ValueError, TypeError):
                            period_values[period_label] = 0.0

                # Create a temporary structure to hold period data
                account_line_item = {
                    "name": cells[0].get("Value", ""),
                    "account_id": account_id,
                    "period_values": period_values,
                    "children": [],
                }
                children.append(account_line_item)

        elif row.get("RowType") == "Section":
            # Recursive parsing for nested sections
            nested_line_item = _parse_balance_sheet_line_item(row, period_labels)
            if nested_line_item:
                children.append(nested_line_item)

        elif row.get("RowType") == "SummaryRow" and row.get("Cells"):
            cells = row["Cells"]
            # Extract period totals (skip first cell which is the label)
            for i in range(1, len(cells)):
                if i - 1 < len(
                    period_labels
                ):  # Ensure we have a corresponding period label
                    period_label = period_labels[i - 1]
                    try:
                        value = float(cells[i].get("Value", "0"))
                        period_totals[period_label] = value
                    except (ValueError, TypeError):
                        period_totals[period_label] = 0.0

    # If no summary row found, calculate totals from children
    if not period_totals and children:
        for period_label in period_labels:
            total = sum(
                child["period_values"].get(period_label, 0.0) for child in children
            )
            period_totals[period_label] = total

    return {
        "name": title,
        "account_id": None,  # This is a grouping, not an individual account
        "period_values": period_totals,
        "children": children,
    }


def _convert_online_invoice_response_to_snake_case(
    data: Dict[str, Any],
) -> Dict[str, Any]:
    """Convert Xero API online invoice response data from PascalCase to snake_case."""
    online_invoices = []
    if data.get("OnlineInvoices"):
        for invoice in data["OnlineInvoices"]:
            online_invoices.append(
                {"online_invoice_url": invoice.get("OnlineInvoiceUrl")}
            )

    return {"online_invoices": online_invoices}



def _xero_contact_to_lutra(data: Dict[str, Any]) -> XeroContact:
    global _XC_TA
    if _XC_TA is None:
        _XC_TA = TypeAdapter(XeroContact)
    converted_data = _convert_contact_to_snake_case(data)
    return _XC_TA.validate_python(converted_data)


def _xero_line_item_to_lutra(data: Dict[str, Any]) -> XeroLineItem:
    global _XC_LI
    if _XC_LI is None:
        _XC_LI = TypeAdapter(XeroLineItem)
    converted_data = _convert_line_item_to_snake_case(data)
    return _XC_LI.validate_python(converted_data)




def _xero_organisation_to_lutra(data: Dict[str, Any]) -> XeroOrganisation:
    global _XO_TA
    if _XO_TA is None:
        _XO_TA = TypeAdapter(XeroOrganisation)
    converted_data = _convert_organisation_to_snake_case(data)
    return _XO_TA.validate_python(converted_data)




def _get_invoice_url(
    short_code: str, invoice_id: str, status: str, type: str
) -> Optional[str]:
    """
    Generate the appropriate Xero invoice URL based on status.

    Args:
        short_code: The organisation short code from _xero_get_organisation()
        invoice_id: The invoice ID
        status: The invoice status (DRAFT, SUBMITTED, AUTHORISED, etc.)

    Returns:
        The appropriate URL for the invoice
    """
    if type == "ACCPAY":
        return f"https://go.xero.com/AccountsPayable/View.aspx?InvoiceID={invoice_id}"
    # Determine the URL path based on status
    if status in ["DRAFT", "SUBMITTED"]:
        path = "edit"
    elif status in ["AUTHORISED", "PAID"]:
        path = "view"
    else:
        # Default to view for other statuses (PAID, VOIDED, etc.)
        path = None

    if path:
        return f"https://go.xero.com/app/{short_code}/invoicing/{path}/{invoice_id}"
    else:
        return None


@purpose("Get tenant id")
async def xero_get_tenant() -> List[XeroTenant]:
    """
    Get the tenant ID and connection ID for the current user.
    """
    response = await xero_client.get(
        "https://api.xero.com/connections",
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()
    return [
        XeroTenant(
            tenant_name=tenant["tenantName"],
            tenant_id=tenant["tenantId"],
            connection_id=tenant["id"],
        )
        for tenant in data
    ]


async def _xero_get_organisation(xero_tenant: XeroTenant) -> XeroOrganisation:
    """
    Get organisation information from Xero including the short code needed for URLs.

    Args:
        xero_tenant: The Xero tenant to get organisation information for

    Returns:
        The Xero organisation information including short_code for URL construction
    """
    response = await xero_client.get(
        "https://api.xero.com/api.xro/2.0/Organisation",
        headers={"Xero-tenant-id": xero_tenant.tenant_id},
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Organisations") or len(data.get("Organisations")) == 0:
        raise ValueError("No organisation data found")

    organisation_data = data.get("Organisations")[0]
    return _xero_organisation_to_lutra(organisation_data)



@purpose("Get a specific invoice from Xero")
async def xero_get_invoice(xero_tenant: XeroTenant, invoice_id: str) -> XeroInvoice:
    """
    Get a specific invoice from Xero by ID.

    Args:
        invoice_id: The ID of the invoice to retrieve

    Returns:
        The Xero invoice with the specified ID
    """
    response = await xero_client.get(
        f"https://api.xero.com/api.xro/2.0/Invoices/{invoice_id}",
        headers={"Xero-tenant-id": xero_tenant.tenant_id},
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Invoices") or len(data.get("Invoices")) == 0:
        raise ValueError(f"Invoice with ID {invoice_id} not found")

    invoice_data = data.get("Invoices")[0]

    # Get organisation info once to get the short code
    try:
        org = await _xero_get_organisation(xero_tenant)
        short_code = org.short_code
    except Exception:
        short_code = ""  # Fallback if org info can't be fetched

    # Generate the appropriate URL based on invoice status
    url = (
        _get_invoice_url(
            short_code,
            invoice_data.get("InvoiceID"),
            invoice_data.get("Status", ""),
            invoice_data.get("Type", ""),
        )
        if short_code
        else None
    )

    return XeroInvoice(
        invoice_id=invoice_data.get("InvoiceID", ""),
        type=invoice_data.get("Type", ""),
        contact=_xero_contact_to_lutra(invoice_data.get("Contact", {})),
        date=_parse_xero_date(invoice_data.get("Date")),
        due_date=_parse_xero_date(invoice_data.get("DueDate")),
        status=invoice_data.get("Status", ""),
        line_amount_types=invoice_data.get("LineAmountTypes", ""),
        line_items=[
            _xero_line_item_to_lutra(item) for item in invoice_data.get("LineItems", [])
        ],
        sub_total=invoice_data.get("SubTotal", 0.0),
        total_tax=invoice_data.get("TotalTax", 0.0),
        total=invoice_data.get("Total", 0.0),
        total_discount=invoice_data.get("TotalDiscount", 0.0),
        invoice_number=invoice_data.get("InvoiceNumber"),
        reference=invoice_data.get("Reference"),
        currency_code=invoice_data.get("CurrencyCode"),
        currency_rate=invoice_data.get("CurrencyRate"),
        branding_theme_id=invoice_data.get("BrandingThemeID"),
        url=url,
        sent_to_contact=invoice_data.get("SentToContact"),
        expected_payment_date=_parse_xero_date(invoice_data.get("ExpectedPaymentDate")),
        planned_payment_date=_parse_xero_date(invoice_data.get("PlannedPaymentDate")),
        has_attachments=invoice_data.get("HasAttachments"),
        repeating_invoice_id=invoice_data.get("RepeatingInvoiceID"),
        amount_due=invoice_data.get("AmountDue"),
        amount_paid=invoice_data.get("AmountPaid"),
        cis_deduction=invoice_data.get("CISDeduction"),
        fully_paid_on_date=_parse_xero_date(invoice_data.get("FullyPaidOnDate")),
        amount_credited=invoice_data.get("AmountCredited"),
        sales_tax_calculation_type_code=invoice_data.get("SalesTaxCalculationTypeCode"),
        invoice_addresses=invoice_data.get("InvoiceAddresses"),
        updated_date_utc=_parse_xero_date(invoice_data.get("UpdatedDateUTC")),
    )


@purpose("Get contacts from Xero")
async def xero_get_contacts(xero_tenant: XeroTenant) -> List[XeroContact]:
    """
    Get all contacts from Xero.

    Args:
        xero_tenant: The Xero tenant to get contacts from

    Returns:
        A list of Xero contacts
    """
    response = await xero_client.get(
        "https://api.xero.com/api.xro/2.0/Contacts",
        headers={"Xero-tenant-id": xero_tenant.tenant_id},
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Contacts"):
        return []

    return [_xero_contact_to_lutra(contact) for contact in data.get("Contacts", [])]


@purpose("Create a contact in Xero")
async def xero_create_contact(
    xero_tenant: XeroTenant,
    name: str,
    contact_number: Optional[str] = None,
    account_number: Optional[str] = None,
    contact_status: Optional[Literal["ACTIVE", "ARCHIVED"]] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    email_address: Optional[str] = None,
    bank_account_details: Optional[str] = None,
    company_number: Optional[str] = None,
    tax_number: Optional[str] = None,
    accounts_receivable_tax_type: Optional[str] = None,
    accounts_payable_tax_type: Optional[str] = None,
    addresses: Optional[List[XeroAddress]] = None,
    phones: Optional[List[XeroPhone]] = None,
    default_currency: Optional[str] = None,
) -> XeroContact:
    """
    Create a contact in Xero. Contacts can be customers (for sales invoices) or suppliers
    (for bills/purchases). The IsSupplier and IsCustomer flags are automatically set when
    invoices are created against the contact.

    Args:
        xero_tenant: The Xero tenant to create the contact in
        name: Full name of contact/organisation (required, max 255 chars)
        contact_number: External system identifier (max 50 chars, read-only in Xero UI)
        account_number: User-defined account number (max 50 chars)
        contact_status: Contact status (defaults to ACTIVE if not specified)
        first_name: First name of contact person (max 255 chars)
        last_name: Last name of contact person (max 255 chars)
        email_address: Email address (max 255 chars, no umlauts)
        bank_account_details: Bank account number
        company_number: Company registration number (max 50 chars)
        tax_number: Tax ID/ABN/GST/VAT number (max 50 chars)
        accounts_receivable_tax_type: Default tax type for sales invoices
        accounts_payable_tax_type: Default tax type for bills
        addresses: List of addresses for the contact
        phones: List of phone numbers for the contact
        default_currency: Default currency for invoices

    Returns:
        The created Xero contact
    """
    # Prepare contact data for API (using PascalCase)
    contact_data = {
        "Name": name,
    }

    # Add optional fields if provided
    if contact_number:
        contact_data["ContactNumber"] = contact_number
    if account_number:
        contact_data["AccountNumber"] = account_number
    if contact_status:
        contact_data["ContactStatus"] = contact_status
    if first_name:
        contact_data["FirstName"] = first_name
    if last_name:
        contact_data["LastName"] = last_name
    if email_address:
        contact_data["EmailAddress"] = email_address
    if bank_account_details:
        contact_data["BankAccountDetails"] = bank_account_details
    if company_number:
        contact_data["CompanyNumber"] = company_number
    if tax_number:
        contact_data["TaxNumber"] = tax_number
    if accounts_receivable_tax_type:
        contact_data["AccountsReceivableTaxType"] = accounts_receivable_tax_type
    if accounts_payable_tax_type:
        contact_data["AccountsPayableTaxType"] = accounts_payable_tax_type
    if default_currency:
        contact_data["DefaultCurrency"] = default_currency

    # Convert addresses to API format if provided
    if addresses:
        contact_data["Addresses"] = []
        for address in addresses:
            addr_dict = {
                "AddressType": address.address_type,
            }
            if address.address_line1:
                addr_dict["AddressLine1"] = address.address_line1
            if address.address_line2:
                addr_dict["AddressLine2"] = address.address_line2
            if address.address_line3:
                addr_dict["AddressLine3"] = address.address_line3
            if address.address_line4:
                addr_dict["AddressLine4"] = address.address_line4
            if address.city:
                addr_dict["City"] = address.city
            if address.region:
                addr_dict["Region"] = address.region
            if address.postal_code:
                addr_dict["PostalCode"] = address.postal_code
            if address.country:
                addr_dict["Country"] = address.country
            if address.attention_to:
                addr_dict["AttentionTo"] = address.attention_to
            contact_data["Addresses"].append(addr_dict)

    # Convert phones to API format if provided
    if phones:
        contact_data["Phones"] = []
        for phone in phones:
            phone_dict = {}
            if phone.phone_number:
                phone_dict["PhoneNumber"] = phone.phone_number
            if phone.phone_type:
                phone_dict["PhoneType"] = phone.phone_type
            if phone.phone_area_code:
                phone_dict["PhoneAreaCode"] = phone.phone_area_code
            if phone.phone_country_code:
                phone_dict["PhoneCountryCode"] = phone.phone_country_code
            contact_data["Phones"].append(phone_dict)

    response = await xero_client.post(
        "https://api.xero.com/api.xro/2.0/Contacts",
        headers={
            "Xero-tenant-id": xero_tenant.tenant_id,
            "Content-Type": "application/json",
        },
        json={"Contacts": [contact_data]},
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Contacts") or len(data.get("Contacts")) == 0:
        raise ValueError("Failed to create contact")

    contact_data = data.get("Contacts")[0]
    return _xero_contact_to_lutra(contact_data)


@purpose("Create an invoice in Xero")
async def xero_create_invoice(
    xero_tenant: XeroTenant,
    xero_contact: XeroContact,
    type: Literal["ACCPAY", "ACCREC"],
    line_items: List[XeroLineItem],
    invoice_date: Optional[datetime] = None,
    due_date: Optional[datetime] = None,
    line_amount_types: Literal["Exclusive", "Inclusive", "NoTax"] = "Exclusive",
    invoice_number: Optional[str] = None,
    reference: Optional[str] = None,
    currency_code: Optional[str] = None,
) -> XeroInvoice:
    """
    Create an invoice in Xero. ACCPAY creates bills (money you owe to suppliers),
    ACCREC creates sales invoices (money customers owe you). Both affect accounts
    receivable/payable and will appear in aging reports.

    MANDATORY WORKFLOW - NO EXCEPTIONS:
    1. ALWAYS show user available accounts using xero_list_accounts() to get the user to select an account.
    2. ALWAYS call xero_get_account() to get the account details for the user selected account.
    3. NEVER assume, auto-select, any account.
    4. NEVER create invoices without confirmed user approval of all account selections
    5. This applies to ALL invoices - no exceptions

    IMPORTANT: Each line item must include an account_code to specify which account
    to post the transaction to. You MUST get the user to specify the account to use for the invoice.
    Without account codes, invoices cannot be approved (moved to AUTHORISED status) and will remain as drafts.

    WORKFLOW EXAMPLE:
    1. Get account summaries: account_summaries = await xero_list_accounts(xero_tenant)
    2. Pause to get the user to select an account.
    3. Fetch the account details: user_selected_account = await xero_get_account(xero_tenant, user_selected_account)
    4. Create line item: line_item = XeroLineItem(..., account_code=user_selected_account.code)
    5. Create invoice with line items

    Args:
        xero_tenant: The Xero tenant to create the invoice in
        xero_contact: Must be marked as supplier (ACCPAY) or customer (ACCREC)
        type: ACCPAY for bills/purchases, ACCREC for sales invoices
        line_items: Line items with account codes
        invoice_date: Transaction date affecting period reporting
        due_date: Payment due date affecting aging calculations
        line_amount_types: Tax treatment - Exclusive/Inclusive affects tax calculations
        invoice_number: Invoice identifier for tracking
        reference: Internal reference for reconciliation
        currency_code: Must match contact's default currency if multi-currency

    Returns:
        The created Xero invoice
    """
    # Default dates if not provided
    if invoice_date is None:
        invoice_date = datetime.now(timezone.utc)
    if due_date is None:
        due_date = invoice_date

    # Convert XeroLineItem objects to dictionaries for API
    line_items_dict = []
    for item in line_items:
        item_dict = {
            "Description": item.description,
            "Quantity": item.quantity,
            "UnitAmount": item.unit_amount,
            "LineAmount": item.line_amount,
            "AccountCode": item.account_code,  # Now required
        }

        # Add optional fields if they exist
        if item.item_code:
            item_dict["ItemCode"] = item.item_code
        if item.tax_type:
            item_dict["TaxType"] = item.tax_type
        if item.tax_amount is not None:
            item_dict["TaxAmount"] = item.tax_amount

        line_items_dict.append(item_dict)

    # Prepare invoice data for API (using PascalCase)
    invoice_data = {
        "Type": type,
        "Contact": {"ContactID": xero_contact.contact_id},
        "Date": invoice_date.isoformat(),
        "DueDate": due_date.isoformat(),
        "LineAmountTypes": line_amount_types,
        "LineItems": line_items_dict,
    }

    # Add optional fields if provided
    if invoice_number:
        invoice_data["InvoiceNumber"] = invoice_number
    if reference:
        invoice_data["Reference"] = reference
    if currency_code:
        invoice_data["CurrencyCode"] = currency_code

    response = await xero_client.post(
        "https://api.xero.com/api.xro/2.0/Invoices",
        headers={
            "Xero-tenant-id": xero_tenant.tenant_id,
            "Content-Type": "application/json",
        },
        json={"Invoices": [invoice_data]},
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Invoices") or len(data.get("Invoices")) == 0:
        raise ValueError("Failed to create invoice")

    invoice_data = data.get("Invoices")[0]

    # Get organisation info once to get the short code
    try:
        org = await _xero_get_organisation(xero_tenant)
        short_code = org.short_code
    except Exception:
        short_code = ""  # Fallback if org info can't be fetched

    # Generate the appropriate URL based on invoice status
    url = (
        _get_invoice_url(
            short_code,
            invoice_data.get("InvoiceID"),
            invoice_data.get("Status", ""),
            invoice_data.get("Type", ""),
        )
        if short_code
        else None
    )

    return XeroInvoice(
        invoice_id=invoice_data.get("InvoiceID", ""),
        type=invoice_data.get("Type", ""),
        contact=_xero_contact_to_lutra(invoice_data.get("Contact")),
        date=_parse_xero_date(invoice_data.get("Date")),
        due_date=_parse_xero_date(invoice_data.get("DueDate")),
        status=invoice_data.get("Status", ""),
        line_amount_types=invoice_data.get("LineAmountTypes", ""),
        line_items=[
            _xero_line_item_to_lutra(item) for item in invoice_data.get("LineItems", [])
        ],
        sub_total=invoice_data.get("SubTotal", 0.0),
        total_tax=invoice_data.get("TotalTax", 0.0),
        total=invoice_data.get("Total", 0.0),
        total_discount=invoice_data.get("TotalDiscount", 0.0),
        invoice_number=invoice_data.get("InvoiceNumber"),
        reference=invoice_data.get("Reference"),
        currency_code=invoice_data.get("CurrencyCode"),
        currency_rate=invoice_data.get("CurrencyRate"),
        branding_theme_id=invoice_data.get("BrandingThemeID"),
        url=url,
        sent_to_contact=invoice_data.get("SentToContact"),
        expected_payment_date=_parse_xero_date(invoice_data.get("ExpectedPaymentDate")),
        planned_payment_date=_parse_xero_date(invoice_data.get("PlannedPaymentDate")),
        has_attachments=invoice_data.get("HasAttachments"),
        repeating_invoice_id=invoice_data.get("RepeatingInvoiceID"),
        amount_due=invoice_data.get("AmountDue"),
        amount_paid=invoice_data.get("AmountPaid"),
        cis_deduction=invoice_data.get("CISDeduction"),
        fully_paid_on_date=_parse_xero_date(invoice_data.get("FullyPaidOnDate")),
        amount_credited=invoice_data.get("AmountCredited"),
        sales_tax_calculation_type_code=invoice_data.get("SalesTaxCalculationTypeCode"),
        invoice_addresses=invoice_data.get("InvoiceAddresses"),
        updated_date_utc=_parse_xero_date(invoice_data.get("UpdatedDateUTC")),
    )


@purpose("Update an invoice in Xero")
async def xero_update_invoice(
    xero_tenant: XeroTenant,
    invoice_id: str,
    type: Optional[Literal["ACCPAY", "ACCREC"]] = None,
    contact: Optional[XeroContact] = None,
    line_items: Optional[List[XeroLineItem]] = None,
    invoice_date: Optional[datetime] = None,
    due_date: Optional[datetime] = None,
    line_amount_types: Optional[Literal["Exclusive", "Inclusive", "NoTax"]] = None,
    invoice_number: Optional[str] = None,
    reference: Optional[str] = None,
    currency_code: Optional[str] = None,
    branding_theme_id: Optional[str] = None,
    url: Optional[str] = None,
    status: Optional[
        Literal["DRAFT", "SUBMITTED", "AUTHORISED", "DELETED", "VOIDED"]
    ] = None,
) -> XeroInvoice:
    """
    Update an invoice in Xero. Update capabilities depend on invoice type and payment status:

    - ACCREC (sales invoices): Can update when unpaid; limited fields when paid
    - ACCPAY (bills): Can update when unpaid; cannot update when paid
    - Invoices in locked periods cannot be updated

    When paid (partially/fully), only these fields can be updated:
    Reference, DueDate, InvoiceNumber, BrandingThemeID, Contact (unless paid with Credit Note),
    URL, LineItems (Description, AccountCode except CIS), Tracking

    Invoice Status Transitions:
    - DRAFT → SUBMITTED, AUTHORISED, DELETED
    - SUBMITTED → AUTHORISED, DRAFT, DELETED
    - AUTHORISED → VOIDED

    MANDATORY WORKFLOW - NO EXCEPTIONS:
    1. ALWAYS show user available accounts using xero_list_accounts() to get the user to select an account.
    2. ALWAYS call xero_get_account() to get the account details for the user selected account.
    3. NEVER assume, auto-select, any account.
    4. NEVER create invoices without confirmed user approval of all account selections
    5. This applies to ALL invoices - no exceptions

    IMPORTANT: To approve an invoice (status=AUTHORISED), all line items must have
    account_code specified. You MUST FORCE the user to approve the account specified in the
    invoice before approving the invoice - do not let them approve the invoice without approving the account
    and do not assume that the account is correct.

    Args:
        xero_tenant: The Xero tenant containing the invoice
        invoice_id: The ID of the invoice to update
        type: Invoice type (ACCPAY for bills, ACCREC for sales invoices)
        contact: Updated contact (not allowed if paid with Credit Note)
        line_items: Updated line items with account codes
        invoice_date: Updated transaction date
        due_date: Updated payment due date
        line_amount_types: Updated tax treatment
        invoice_number: Updated invoice identifier
        reference: Updated internal reference
        currency_code: Updated currency (must match contact's default if multi-currency)
        branding_theme_id: Updated branding theme
        url: Updated invoice URL
        status: Updated invoice status (see valid transitions above)

    Returns:
        The updated Xero invoice

    Raises:
        ValueError: If update fails due to payment status, field restrictions, or invalid status transition
    """
    # Prepare invoice data for API (using PascalCase)
    invoice_data = {}

    # Add fields if provided
    if type is not None:
        invoice_data["Type"] = type
    if contact is not None:
        invoice_data["Contact"] = {"ContactID": contact.contact_id}
    if invoice_date is not None:
        invoice_data["Date"] = invoice_date.isoformat()
    if due_date is not None:
        invoice_data["DueDate"] = due_date.isoformat()
    if line_amount_types is not None:
        invoice_data["LineAmountTypes"] = line_amount_types
    if invoice_number is not None:
        invoice_data["InvoiceNumber"] = invoice_number
    if reference is not None:
        invoice_data["Reference"] = reference
    if currency_code is not None:
        invoice_data["CurrencyCode"] = currency_code
    if branding_theme_id is not None:
        invoice_data["BrandingThemeID"] = branding_theme_id
    if url is not None:
        invoice_data["Url"] = url
    if status is not None:
        invoice_data["Status"] = status

    # Convert XeroLineItem objects to dictionaries for API if provided
    if line_items is not None:
        line_items_dict = []
        for item in line_items:
            item_dict = {
                "Description": item.description,
                "Quantity": item.quantity,
                "UnitAmount": item.unit_amount,
                "LineAmount": item.line_amount,
                "AccountCode": item.account_code,  # Now required
            }

            # Add optional fields if they exist
            if item.item_code:
                item_dict["ItemCode"] = item.item_code
            if item.tax_type:
                item_dict["TaxType"] = item.tax_type
            if item.tax_amount is not None:
                item_dict["TaxAmount"] = item.tax_amount
            if item.line_item_id:
                item_dict["LineItemID"] = item.line_item_id

            line_items_dict.append(item_dict)

        invoice_data["LineItems"] = line_items_dict

    response = await xero_client.post(
        f"https://api.xero.com/api.xro/2.0/Invoices/{invoice_id}",
        headers={
            "Xero-tenant-id": xero_tenant.tenant_id,
            "Content-Type": "application/json",
        },
        json={"Invoices": [invoice_data]},
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Invoices") or len(data.get("Invoices")) == 0:
        raise ValueError(f"Failed to update invoice with ID {invoice_id}")

    invoice_data = data.get("Invoices")[0]

    # Get organisation info once to get the short code
    try:
        org = await _xero_get_organisation(xero_tenant)
        short_code = org.short_code
    except Exception:
        short_code = ""  # Fallback if org info can't be fetched

    # Generate the appropriate URL based on invoice status
    url = (
        _get_invoice_url(
            short_code,
            invoice_data.get("InvoiceID"),
            invoice_data.get("Status", ""),
            invoice_data.get("Type", ""),
        )
        if short_code
        else None
    )

    return XeroInvoice(
        invoice_id=invoice_data.get("InvoiceID", ""),
        type=invoice_data.get("Type", ""),
        contact=_xero_contact_to_lutra(invoice_data.get("Contact")),
        date=_parse_xero_date(invoice_data.get("Date")),
        due_date=_parse_xero_date(invoice_data.get("DueDate")),
        status=invoice_data.get("Status", ""),
        line_amount_types=invoice_data.get("LineAmountTypes", ""),
        line_items=[
            _xero_line_item_to_lutra(item) for item in invoice_data.get("LineItems", [])
        ],
        sub_total=invoice_data.get("SubTotal", 0.0),
        total_tax=invoice_data.get("TotalTax", 0.0),
        total=invoice_data.get("Total", 0.0),
        total_discount=invoice_data.get("TotalDiscount", 0.0),
        invoice_number=invoice_data.get("InvoiceNumber"),
        reference=invoice_data.get("Reference"),
        currency_code=invoice_data.get("CurrencyCode"),
        currency_rate=invoice_data.get("CurrencyRate"),
        branding_theme_id=invoice_data.get("BrandingThemeID"),
        url=url,
        sent_to_contact=invoice_data.get("SentToContact"),
        expected_payment_date=_parse_xero_date(invoice_data.get("ExpectedPaymentDate")),
        planned_payment_date=_parse_xero_date(invoice_data.get("PlannedPaymentDate")),
        has_attachments=invoice_data.get("HasAttachments"),
        repeating_invoice_id=invoice_data.get("RepeatingInvoiceID"),
        amount_due=invoice_data.get("AmountDue"),
        amount_paid=invoice_data.get("AmountPaid"),
        cis_deduction=invoice_data.get("CISDeduction"),
        fully_paid_on_date=_parse_xero_date(invoice_data.get("FullyPaidOnDate")),
        amount_credited=invoice_data.get("AmountCredited"),
        sales_tax_calculation_type_code=invoice_data.get("SalesTaxCalculationTypeCode"),
        invoice_addresses=invoice_data.get("InvoiceAddresses"),
        updated_date_utc=_parse_xero_date(invoice_data.get("UpdatedDateUTC")),
    )


@purpose("List invoices from Xero")
async def xero_list_invoices(
    xero_tenant: XeroTenant,
    pagination_token: Optional[XeroPaginationToken] = None,
    status: Optional[
        Literal["DRAFT", "SUBMITTED", "AUTHORISED", "DELETED", "VOIDED", "PAID"]
    ] = None,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    contact_id: Optional[str] = None,
) -> tuple[List[XeroInvoice], Optional[XeroPaginationToken]]:
    """
    List invoices from Xero with pagination and optional filtering.

    Args:
        xero_tenant: The Xero tenant to get invoices from
        pagination_token: Page number for pagination (starts at 1)
        status: Filter by invoice status (DRAFT, SUBMITTED, AUTHORISED, etc.)
        from_date: Filter invoices from this date (based on invoice date)
        to_date: Filter invoices to this date (based on invoice date)
        contact_id: Filter invoices for a specific contact

    Returns:
        A tuple containing:
        - List of Xero invoices (maximum 100 per request)
        - Next pagination token (None if no more invoices available)
    """
    params = {}

    # Add pagination
    if pagination_token is not None:
        params["page"] = pagination_token.offset

    # Build where clause for filtering
    where_conditions = []

    if status:
        where_conditions.append(f'Status=="{status}"')

    if from_date:
        where_conditions.append(
            f"Date >= DateTime({from_date.year}, {from_date.month:02d}, {from_date.day:02d})"
        )

    if to_date:
        where_conditions.append(
            f"Date <= DateTime({to_date.year}, {to_date.month:02d}, {to_date.day:02d})"
        )

    if contact_id:
        where_conditions.append(f'Contact.ContactID.ToString()=="{contact_id}"')

    if where_conditions:
        where_clause = " && ".join(where_conditions)
        params["where"] = urllib.parse.quote(where_clause)

    response = await xero_client.get(
        "https://api.xero.com/api.xro/2.0/Invoices",
        headers={"Xero-tenant-id": xero_tenant.tenant_id},
        params=params if params else None,
    )
    await raise_error_text(response)
    await response.aread()
    data = response.json()

    if not data.get("Invoices"):
        return [], None

    # Get organisation info once to get the short code for all invoices
    try:
        org = await _xero_get_organisation(xero_tenant)
        short_code = org.short_code
    except Exception:
        short_code = ""  # Fallback if org info can't be fetched

    # Convert each invoice and generate URLs
    invoices = []
    for invoice_data in data.get("Invoices", []):
        # Generate the appropriate URL based on invoice status
        url = (
            _get_invoice_url(
                short_code,
                invoice_data.get("InvoiceID"),
                invoice_data.get("Status", ""),
                invoice_data.get("Type", ""),
            )
            if short_code
            else None
        )

        invoice = XeroInvoice(
            invoice_id=invoice_data.get("InvoiceID", ""),
            type=invoice_data.get("Type", ""),
            contact=_xero_contact_to_lutra(invoice_data.get("Contact", {})),
            date=_parse_xero_date(invoice_data.get("Date")),
            due_date=_parse_xero_date(invoice_data.get("DueDate")),
            status=invoice_data.get("Status", ""),
            line_amount_types=invoice_data.get("LineAmountTypes", ""),
            line_items=[
                _xero_line_item_to_lutra(item)
                for item in invoice_data.get("LineItems", [])
            ],
            sub_total=invoice_data.get("SubTotal", 0.0),
            total_tax=invoice_data.get("TotalTax", 0.0),
            total=invoice_data.get("Total", 0.0),
            total_discount=invoice_data.get("TotalDiscount", 0.0),
            invoice_number=invoice_data.get("InvoiceNumber"),
            reference=invoice_data.get("Reference"),
            currency_code=invoice_data.get("CurrencyCode"),
            currency_rate=invoice_data.get("CurrencyRate"),
            branding_theme_id=invoice_data.get("BrandingThemeID"),
            url=url,
            sent_to_contact=invoice_data.get("SentToContact"),
            expected_payment_date=_parse_xero_date(
                invoice_data.get("ExpectedPaymentDate")
            ),
            planned_payment_date=_parse_xero_date(
                invoice_data.get("PlannedPaymentDate")
            ),
            has_attachments=invoice_data.get("HasAttachments"),
            repeating_invoice_id=invoice_data.get("RepeatingInvoiceID"),
            amount_due=invoice_data.get("AmountDue"),
            amount_paid=invoice_data.get("AmountPaid"),
            cis_deduction=invoice_data.get("CISDeduction"),
            fully_paid_on_date=_parse_xero_date(invoice_data.get("FullyPaidOnDate")),
            amount_credited=invoice_data.get("AmountCredited"),
            sales_tax_calculation_type_code=invoice_data.get(
                "SalesTaxCalculationTypeCode"
            ),
            invoice_addresses=invoice_data.get("InvoiceAddresses"),
            updated_date_utc=_parse_xero_date(invoice_data.get("UpdatedDateUTC")),
        )
        invoices.append(invoice)

    # Create next pagination token if we got a full page (100 invoices)
    next_token = None
    if len(invoices) == 100:  # Full page indicates more results may be available
        current_page = pagination_token.offset if pagination_token else 1
        next_token = XeroPaginationToken(offset=current_page + 1)

    return invoices, next_token


