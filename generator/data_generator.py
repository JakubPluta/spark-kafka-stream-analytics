import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Generator, List, Optional

import faker
import numpy as np

fake = faker.Faker()


# Enums for better data consistency
class EmploymentType(str, Enum):
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"
    SELF_EMPLOYED = "SELF_EMPLOYED"
    CONTRACTOR = "CONTRACTOR"
    UNEMPLOYED = "UNEMPLOYED"
    RETIRED = "RETIRED"


class EducationLevel(str, Enum):
    HIGH_SCHOOL = "HIGH_SCHOOL"
    BACHELOR = "BACHELOR"
    MASTER = "MASTER"
    PHD = "PHD"
    OTHER = "OTHER"


class LoanPurpose(str, Enum):
    MORTGAGE = "MORTGAGE"
    CAR_LOAN = "CAR_LOAN"
    PERSONAL_LOAN = "PERSONAL_LOAN"
    BUSINESS_LOAN = "BUSINESS_LOAN"
    EDUCATION_LOAN = "EDUCATION_LOAN"
    DEBT_CONSOLIDATION = "DEBT_CONSOLIDATION"


class ApplicationChannel(str, Enum):
    MOBILE_APP = "MOBILE_APP"
    WEB = "WEB"
    BRANCH = "BRANCH"
    PHONE = "PHONE"
    PARTNER = "PARTNER"


@dataclass
class Address:
    street: str
    city: str
    state: str
    postal_code: str
    country: str
    longitude: float
    latitude: float
    residence_duration_months: int


@dataclass
class FinancialProfile:
    monthly_income: float
    annual_income: float
    employment_type: EmploymentType
    employer_name: str
    job_title: str
    employment_duration_months: int
    credit_score: int
    existing_loans_count: int
    total_existing_loan_amount: float
    monthly_expenses: float
    savings_balance: float
    checking_balance: float
    investment_portfolio_value: float
    last_year_tax_return_amount: float
    bankruptcy_history: bool
    last_bankruptcy_date: Optional[str]
    debt_to_income_ratio: float


@dataclass
class BehavioralMetrics:
    average_monthly_transactions: int
    last_month_transactions: int
    average_transaction_value: float
    largest_transaction_amount: float
    frequent_merchant_categories: List[str]
    mobile_banking_usage_score: int
    card_payment_frequency: int
    atm_withdrawal_frequency: int
    international_transactions_frequency: int
    missed_payment_count_last_year: int
    overdraft_frequency: int
    investment_risk_score: int
    savings_frequency: int
    customer_service_calls_count: int
    fraud_alerts_count: int


@dataclass
class CustomerProfile:
    customer_id: int
    first_name: str
    last_name: str
    email: str
    phone_number: str
    date_of_birth: str
    age: int
    gender: str
    nationality: str
    marital_status: str
    number_of_dependents: int
    education_level: EducationLevel
    occupation: str
    customer_segment: str
    relationship_tenure_months: int
    address: Address
    financial_profile: FinancialProfile
    behavioral_metrics: BehavioralMetrics
    preferred_contact_method: str
    language_preference: str
    social_media_presence_score: int
    customer_lifetime_value: float
    churn_risk_score: float
    credit_product_count: int
    insurance_product_count: int
    investment_product_count: int
    last_contact_date: str
    marketing_campaign_responses: int
    referral_count: int
    satisfaction_score: float
    digital_banking_status: bool
    vip_status: bool
    loyalty_program_tier: str
    last_feedback_date: Optional[str]
    feedback_sentiment_score: Optional[float]
    preferred_banking_channel: str
    risk_category: str
    kyc_status: str
    last_kyc_update_date: str
    document_verification_status: str
    privacy_preferences: Dict[str, bool]
    notification_preferences: Dict[str, bool]
    linked_accounts_count: int
    device_preferences: List[str]
    average_product_holding_duration: int
    product_recommendation_score: float
    cross_sell_eligibility_score: float
    last_product_purchase_date: Optional[str]
    fraud_check_status: str
    credit_limit_utilization: float
    last_credit_review_date: str


@dataclass
class LoanApplicationEvent:
    event_id: str
    customer_id: int
    timestamp: str
    application_channel: ApplicationChannel
    loan_purpose: LoanPurpose
    requested_amount: float
    requested_term_months: int
    proposed_interest_rate: float
    collateral_type: Optional[str]
    collateral_value: Optional[float]
    cosigner_present: bool
    cosigner_credit_score: Optional[int]
    monthly_payment: float
    debt_to_income_ratio_after_loan: float
    application_score: float
    risk_assessment_score: float
    automated_decision: str
    required_documents: List[str]
    missing_documents: List[str]
    credit_check_consent: bool
    employment_verification_status: str
    income_verification_status: str
    identity_verification_score: float
    fraud_check_result: str
    device_fingerprint: str
    ip_address: str
    session_duration: int
    number_of_attempts: int
    previous_applications_count: int
    marketing_campaign_id: Optional[str]
    partner_referral_id: Optional[str]
    application_completion_time: int
    user_agent: str
    browser_language: str
    geo_location: Dict[str, float]
    device_type: str
    correlation_id: str
    batch_id: str


class DataGenerator:
    def __init__(self):
        self.fake = faker.Faker()
        random.seed(42)
        np.random.seed(42)

    def generate_address(self) -> Address:
        return Address(
            street=self.fake.street_address(),
            city=self.fake.city(),
            state=self.fake.state(),
            postal_code=self.fake.zipcode(),
            country="United States",
            longitude=float(self.fake.longitude()),
            latitude=float(self.fake.latitude()),
            residence_duration_months=random.randint(1, 240),
        )

    def generate_financial_profile(self) -> FinancialProfile:
        monthly_income = round(random.uniform(3000, 25000), 2)
        bankruptcy_history = random.random() < 0.05

        return FinancialProfile(
            monthly_income=monthly_income,
            annual_income=monthly_income * 12,
            employment_type=random.choice(list(EmploymentType)),
            employer_name=self.fake.company(),
            job_title=self.fake.job(),
            employment_duration_months=random.randint(0, 300),
            credit_score=random.randint(300, 850),
            existing_loans_count=random.randint(0, 5),
            total_existing_loan_amount=round(random.uniform(0, 500000), 2),
            monthly_expenses=round(monthly_income * random.uniform(0.3, 0.8), 2),
            savings_balance=round(random.uniform(0, 100000), 2),
            checking_balance=round(random.uniform(0, 50000), 2),
            investment_portfolio_value=round(random.uniform(0, 200000), 2),
            last_year_tax_return_amount=round(random.uniform(-10000, 50000), 2),
            bankruptcy_history=bankruptcy_history,
            last_bankruptcy_date=self.fake.date_between(
                start_date="-10y", end_date="today"
            ).isoformat()
            if bankruptcy_history
            else None,
            debt_to_income_ratio=round(random.uniform(0.1, 0.6), 2),
        )

    def generate_behavioral_metrics(self) -> BehavioralMetrics:
        return BehavioralMetrics(
            average_monthly_transactions=random.randint(20, 200),
            last_month_transactions=random.randint(15, 250),
            average_transaction_value=round(random.uniform(20, 500), 2),
            largest_transaction_amount=round(random.uniform(1000, 10000), 2),
            frequent_merchant_categories=random.sample(
                [
                    "Groceries",
                    "Dining",
                    "Entertainment",
                    "Shopping",
                    "Travel",
                    "Healthcare",
                    "Utilities",
                    "Education",
                ],
                random.randint(3, 6),
            ),
            mobile_banking_usage_score=random.randint(1, 100),
            card_payment_frequency=random.randint(10, 100),
            atm_withdrawal_frequency=random.randint(0, 20),
            international_transactions_frequency=random.randint(0, 10),
            missed_payment_count_last_year=random.randint(0, 5),
            overdraft_frequency=random.randint(0, 10),
            investment_risk_score=random.randint(1, 100),
            savings_frequency=random.randint(0, 30),
            customer_service_calls_count=random.randint(0, 20),
            fraud_alerts_count=random.randint(0, 3),
        )

    def generate_customer_profile(self, customer_id: int) -> CustomerProfile:
        dob = self.fake.date_of_birth(minimum_age=18, maximum_age=90)
        age = (
            datetime.now() - datetime.strptime(dob.isoformat(), "%Y-%m-%d")
        ).days // 365

        return CustomerProfile(
            customer_id=customer_id,
            first_name=self.fake.first_name(),
            last_name=self.fake.last_name(),
            email=self.fake.email(),
            phone_number=self.fake.phone_number(),
            date_of_birth=dob.isoformat(),
            age=age,
            gender=random.choice(["M", "F", "Other"]),
            nationality=self.fake.country(),
            marital_status=random.choice(["Single", "Married", "Divorced", "Widowed"]),
            number_of_dependents=random.randint(0, 5),
            education_level=random.choice(list(EducationLevel)),
            occupation=self.fake.job(),
            customer_segment=random.choice(
                ["Mass", "Mass Affluent", "Affluent", "High Net Worth"]
            ),
            relationship_tenure_months=random.randint(1, 240),
            address=self.generate_address(),
            financial_profile=self.generate_financial_profile(),
            behavioral_metrics=self.generate_behavioral_metrics(),
            preferred_contact_method=random.choice(["Email", "Phone", "SMS", "Mail"]),
            language_preference=random.choice(
                ["English", "Spanish", "French", "Chinese"]
            ),
            social_media_presence_score=random.randint(1, 100),
            customer_lifetime_value=round(random.uniform(1000, 1000000), 2),
            churn_risk_score=round(random.uniform(0, 1), 2),
            credit_product_count=random.randint(0, 5),
            insurance_product_count=random.randint(0, 3),
            investment_product_count=random.randint(0, 4),
            last_contact_date=(
                datetime.now() - timedelta(days=random.randint(0, 365))
            ).isoformat(),
            marketing_campaign_responses=random.randint(0, 20),
            referral_count=random.randint(0, 10),
            satisfaction_score=round(random.uniform(1, 5), 1),
            digital_banking_status=random.choice([True, False]),
            vip_status=random.random() < 0.1,
            loyalty_program_tier=random.choice(
                ["Bronze", "Silver", "Gold", "Platinum"]
            ),
            last_feedback_date=(
                datetime.now() - timedelta(days=random.randint(0, 180))
            ).isoformat(),
            feedback_sentiment_score=round(random.uniform(-1, 1), 2),
            preferred_banking_channel=random.choice(["Mobile", "Web", "Branch", "ATM"]),
            risk_category=random.choice(["Low", "Medium", "High"]),
            kyc_status=random.choice(["Verified", "Pending", "Review Required"]),
            last_kyc_update_date=(
                datetime.now() - timedelta(days=random.randint(0, 730))
            ).isoformat(),
            document_verification_status=random.choice(
                ["Complete", "Incomplete", "Expired"]
            ),
            privacy_preferences={
                "marketing_emails": random.choice([True, False]),
                "third_party_sharing": random.choice([True, False]),
                "data_analytics": random.choice([True, False]),
            },
            notification_preferences={
                "transaction_alerts": random.choice([True, False]),
                "balance_alerts": random.choice([True, False]),
                "security_alerts": random.choice([True, False]),
                "marketing_notifications": random.choice([True, False]),
            },
            linked_accounts_count=random.randint(0, 5),
            device_preferences=random.sample(
                ["Mobile", "Tablet", "Desktop", "Smart Watch"], random.randint(1, 3)
            ),
            average_product_holding_duration=random.randint(1, 120),
            product_recommendation_score=round(random.uniform(0, 1), 2),
            cross_sell_eligibility_score=round(random.uniform(0, 1), 2),
            last_product_purchase_date=(
                datetime.now() - timedelta(days=random.randint(0, 365))
            ).isoformat(),
            fraud_check_status=random.choice(["Passed", "Failed", "Review Required"]),
            credit_limit_utilization=round(random.uniform(0, 1), 2),
            last_credit_review_date=(
                datetime.now() - timedelta(days=random.randint(0, 365))
            ).isoformat(),
        )

    def generate_loan_application_event(self, customer_id: int) -> LoanApplicationEvent:
        requested_amount = round(random.uniform(5000, 500000), 2)
        requested_term_months = random.choice([12, 24, 36, 48, 60, 72, 84, 96, 120])

        return LoanApplicationEvent(
            event_id=str(uuid.uuid4()),
            customer_id=customer_id,
            timestamp=datetime.now().isoformat(),
            application_channel=random.choice(list(ApplicationChannel)),
            loan_purpose=random.choice(list(LoanPurpose)),
            requested_amount=requested_amount,
            requested_term_months=requested_term_months,
            proposed_interest_rate=round(random.uniform(0.029, 0.15), 3),
            collateral_type=random.choice(
                [None, "Vehicle", "Property", "Investment", "Other"]
            ),
            collateral_value=round(
                random.uniform(requested_amount * 1.2, requested_amount * 2), 2
            )
            if random.random() > 0.3
            else None,
            cosigner_present=random.random() < 0.2,
            cosigner_credit_score=random.randint(600, 850)
            if random.random() < 0.2
            else None,
            monthly_payment=round(
                requested_amount
                / requested_term_months
                * (1 + random.uniform(0.1, 0.3)),
                2,
            ),
            debt_to_income_ratio_after_loan=round(random.uniform(0.2, 0.8), 2),
            application_score=round(random.uniform(300, 850), 2),
            risk_assessment_score=round(random.uniform(1, 100), 2),
            automated_decision=random.choice(
                ["APPROVED", "REJECTED", "MANUAL_REVIEW", "PENDING"]
            ),
            required_documents=[
                "ID_PROOF",
                "ADDRESS_PROOF",
                "INCOME_PROOF",
                "BANK_STATEMENTS",
                "TAX_RETURNS",
            ],
            missing_documents=random.sample(
                [
                    "ID_PROOF",
                    "ADDRESS_PROOF",
                    "INCOME_PROOF",
                    "BANK_STATEMENTS",
                    "TAX_RETURNS",
                ],
                random.randint(0, 3),
            ),
            credit_check_consent=True,
            employment_verification_status=random.choice(
                ["VERIFIED", "PENDING", "FAILED", "NOT_REQUIRED"]
            ),
            income_verification_status=random.choice(
                ["VERIFIED", "PENDING", "FAILED", "NOT_REQUIRED"]
            ),
            identity_verification_score=round(random.uniform(0.1, 1.0), 2),
            fraud_check_result=random.choice(["PASS", "FLAG", "REVIEW", "FAIL"]),
            device_fingerprint=str(uuid.uuid4()),
            ip_address=fake.ipv4(),
            session_duration=random.randint(60, 3600),
            number_of_attempts=random.randint(1, 5),
            previous_applications_count=random.randint(0, 5),
            marketing_campaign_id=f"CAMP_{random.randint(1000, 9999)}"
            if random.random() < 0.3
            else None,
            partner_referral_id=f"REF_{random.randint(1000, 9999)}"
            if random.random() < 0.2
            else None,
            application_completion_time=random.randint(300, 1800),
            user_agent=fake.user_agent(),
            browser_language=random.choice(["en-US", "en-GB", "es-ES", "fr-FR"]),
            geo_location={
                "latitude": float(fake.latitude()),
                "longitude": float(fake.longitude()),
            },
            device_type=random.choice(["MOBILE", "DESKTOP", "TABLET"]),
            correlation_id=str(uuid.uuid4()),
            batch_id=f"BATCH_{datetime.now().strftime('%Y%m%d')}_{random.randint(1000, 9999)}",
        )

    def generate_customer_data(
        self, num_customers: int
    ) -> Generator[CustomerProfile, None, None]:
        """Generate customer profiles for the entire customer base"""
        for customer_id in range(1, num_customers + 1):
            yield self.generate_customer_profile(customer_id)

    def generate_loan_events(
        self, num_events: int, customer_ids: List[int]
    ) -> List[LoanApplicationEvent]:
        """Generate random loan application events for existing customers"""
        return [
            self.generate_loan_application_event(random.choice(customer_ids))
            for _ in range(num_events)
        ]
