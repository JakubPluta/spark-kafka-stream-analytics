from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    ArrayType,
    MapType,
    DateType,
    TimestampType,
)

AddressSchema = StructType(
    [
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("residence_duration_months", IntegerType(), True),
    ]
)

FinancialProfileSchema = StructType(
    [
        StructField("monthly_income", FloatType(), True),
        StructField("annual_income", FloatType(), True),
        StructField("employment_type", StringType(), True),
        StructField("employer_name", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("employment_duration_months", IntegerType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("existing_loans_count", IntegerType(), True),
        StructField("total_existing_loan_amount", FloatType(), True),
        StructField("monthly_expenses", FloatType(), True),
        StructField("savings_balance", FloatType(), True),
        StructField("checking_balance", FloatType(), True),
        StructField("investment_portfolio_value", FloatType(), True),
        StructField("last_year_tax_return_amount", FloatType(), True),
        StructField("bankruptcy_history", BooleanType(), True),
        StructField("last_bankruptcy_date", StringType(), True),
        StructField("debt_to_income_ratio", FloatType(), True),
    ]
)


BehavioralMetricsSchema = StructType(
    [
        StructField("average_monthly_transactions", IntegerType(), True),
        StructField("last_month_transactions", IntegerType(), True),
        StructField("average_transaction_value", FloatType(), True),
        StructField("largest_transaction_amount", FloatType(), True),
        StructField("frequent_merchant_categories", ArrayType(StringType()), True),
        StructField("mobile_banking_usage_score", IntegerType(), True),
        StructField("card_payment_frequency", IntegerType(), True),
        StructField("atm_withdrawal_frequency", IntegerType(), True),
        StructField("international_transactions_frequency", IntegerType(), True),
        StructField("missed_payment_count_last_year", IntegerType(), True),
        StructField("overdraft_frequency", IntegerType(), True),
        StructField("investment_risk_score", IntegerType(), True),
        StructField("savings_frequency", IntegerType(), True),
        StructField("customer_service_calls_count", IntegerType(), True),
        StructField("fraud_alerts_count", IntegerType(), True),
    ]
)


LoanApplicationSchema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("application_channel", StringType(), True),  # ApplicationChannel
        StructField("loan_purpose", StringType(), True),  # LoanPurpose
        StructField("requested_amount", FloatType(), True),
        StructField("requested_term_months", IntegerType(), True),
        StructField("proposed_interest_rate", FloatType(), True),
        StructField("collateral_type", StringType(), True),
        StructField("collateral_value", FloatType(), True),
        StructField("cosigner_present", BooleanType(), True),
        StructField("cosigner_credit_score", IntegerType(), True),
        StructField("monthly_payment", FloatType(), True),
        StructField("debt_to_income_ratio_after_loan", FloatType(), True),
        StructField("application_score", FloatType(), True),
        StructField("risk_assessment_score", FloatType(), True),
        StructField("automated_decision", StringType(), True),
        StructField("required_documents", ArrayType(StringType()), True),
        StructField("missing_documents", ArrayType(StringType()), True),
        StructField("credit_check_consent", BooleanType(), True),
        StructField("employment_verification_status", StringType(), True),
        StructField("income_verification_status", StringType(), True),
        StructField("identity_verification_score", FloatType(), True),
        StructField("fraud_check_result", StringType(), True),
        StructField("device_fingerprint", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("session_duration", IntegerType(), True),
        StructField("number_of_attempts", IntegerType(), True),
        StructField("previous_applications_count", IntegerType(), True),
        StructField("marketing_campaign_id", StringType(), True),
        StructField("partner_referral_id", StringType(), True),
        StructField("application_completion_time", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("browser_language", StringType(), True),
        StructField("geo_location", MapType(StringType(), FloatType()), True),
        StructField("device_type", StringType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("batch_id", StringType(), True),
    ]
)


CustomerProfileSchema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("marital_status", StringType(), True),
        StructField("number_of_dependents", IntegerType(), True),
        StructField("education_level", StringType(), True),  # EducationLevel
        StructField("occupation", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("relationship_tenure_months", IntegerType(), True),
        StructField("address", AddressSchema, True),
        StructField("financial_profile", FinancialProfileSchema, True),
        StructField("behavioral_metrics", BehavioralMetricsSchema, True),
        StructField("preferred_contact_method", StringType(), True),
        StructField("language_preference", StringType(), True),
        StructField("social_media_presence_score", IntegerType(), True),
        StructField("customer_lifetime_value", FloatType(), True),
        StructField("churn_risk_score", FloatType(), True),
        StructField("credit_product_count", IntegerType(), True),
        StructField("insurance_product_count", IntegerType(), True),
        StructField("investment_product_count", IntegerType(), True),
        StructField("last_contact_date", StringType(), True),
        StructField("marketing_campaign_responses", IntegerType(), True),
        StructField("referral_count", IntegerType(), True),
        StructField("satisfaction_score", FloatType(), True),
        StructField("digital_banking_status", BooleanType(), True),
        StructField("vip_status", BooleanType(), True),
        StructField("loyalty_program_tier", StringType(), True),
        StructField("last_feedback_date", StringType(), True),
        StructField("feedback_sentiment_score", FloatType(), True),
        StructField("preferred_banking_channel", StringType(), True),
        StructField("risk_category", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("last_kyc_update_date", StringType(), True),
        StructField("document_verification_status", StringType(), True),
        StructField("privacy_preferences", MapType(StringType(), BooleanType()), True),
        StructField(
            "notification_preferences", MapType(StringType(), BooleanType()), True
        ),
        StructField("linked_accounts_count", IntegerType(), True),
        StructField("device_preferences", ArrayType(StringType()), True),
        StructField("average_product_holding_duration", IntegerType(), True),
        StructField("product_recommendation_score", FloatType(), True),
        StructField("cross_sell_eligibility_score", FloatType(), True),
        StructField("last_product_purchase_date", StringType(), True),
        StructField("fraud_check_status", StringType(), True),
        StructField("credit_limit_utilization", FloatType(), True),
        StructField("last_credit_review_date", StringType(), True),
    ]
)


LoanApplicationSchemaWithCustomerProfile = StructType(
    LoanApplicationSchema.fields
    + [StructField("customer_profile", CustomerProfileSchema, True)]
)
