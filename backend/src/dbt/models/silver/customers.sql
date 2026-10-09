{{ config(materialized='table') }}

select
    customer_id,
    INITCAP(first_name) as first_name,
    INITCAP(last_name) as last_name,
    {{ clean_email('email') }} AS email,
    {{ email_status('email') }} AS email_status,
    CAST(load_timestamp as timestamp) as load_timestamp,
    {{ normalize_null('gender') }} as gender,
    UPPER(nationality) as nationality,
    {{ match_seed('city', 'seed_city', 'city_fuzzy', 'city_clean') }} as city,
    UPPER(country) as country,
    {{ normalize_null('address') }} as address,
    CAST(lat as float),
    CAST(lon as float),
    {{ format_date('registration_date') }} as registration_date,
    -- Derived: tenure (from registration_date)
    (CURRENT_DATE - {{format_date('registration_date')}})::integer as tenure_days,
    DATE_PART('year', AGE({{format_date('registration_date')}}))::integer as tenure_years,
    LOWER(kyc_status) as kyc_status,
    CAST(risk_score as float) as risk_score,
    -- Derived: risk_tier (from risk_score)
    CASE
        WHEN CAST(risk_score as float) < 26 THEN 'low'
        WHEN CAST(risk_score as float) < 51  THEN 'medium'
        WHEN CAST(risk_score as float) < 76  THEN 'high'
        WHEN CAST(risk_score as float) <= 100 THEN 'critical'
        ELSE NULL
    END as risk_tier,
    {{match_seed('customer_segment', 'seed_customer_segment', 'customer_segment_fuzzy', 'customer_segment_clean')}} as customer_segment,
    INITCAP({{normalize_null('relationship_manager')}}) as relationship_manager,
    {{match_seed('status', 'seed_status', 'status_fuzzy', 'status_clean')}} as status,
    {{normalize_phone('phone_number','country')}} as phone_number,
    {{format_date('date_of_birth')}} as date_of_birth,
    -- Derived: age & age_bucket (from date_of_birth)
    DATE_PART('year', AGE({{format_date('date_of_birth')}}))::integer as age,
    CASE
        WHEN DATE_PART('year', AGE({{format_date('date_of_birth')}})) BETWEEN 18 AND 25 THEN '18-25'
        WHEN DATE_PART('year', AGE({{format_date('date_of_birth')}})) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATE_PART('year', AGE({{format_date('date_of_birth')}})) BETWEEN 36 AND 50 THEN '36-50'
        WHEN DATE_PART('year', AGE({{format_date('date_of_birth')}})) BETWEEN 51 AND 65 THEN '51-65'
        WHEN DATE_PART('year', AGE({{format_date('date_of_birth')}})) > 65 THEN '65+'
        ELSE NULL
    END as age_bucket
from {{ source('silver', 'stg_customers') }}
