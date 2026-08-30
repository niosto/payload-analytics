{{ config(materialized='table') }}

select
    TRIM(customer_id) as customer_id,
    {{normalize_boolean("(digital_engagement::jsonb)->>'mobile_app_registered'")}} as mobile_app_registered,
    {{normalize_boolean("(digital_engagement::jsonb)->>'web_banking_registered'")}} as web_banking_registered,
    {{ format_date("(digital_engagement::jsonb)->>'last_login_date'") }} as last_login_date,
    CAST({{ normalize_null("(digital_engagement::jsonb)->>'avg_monthly_logins'") }} as integer) as avg_monthly_logins,
    (digital_engagement::jsonb)->>'preferred_channel' as preferred_channel,
    {{normalize_boolean("(digital_engagement::jsonb)->>'push_notifications'")}} as push_notifications,
    {{normalize_boolean("(digital_engagement::jsonb)->>'paperless_statements'")}} as paperless_statements,
    CAST(load_timestamp as timestamp) as load_timestamp,
    -- Derived: is_digital (preferred_channel is mobile or web)
    (digital_engagement::jsonb)->>'preferred_channel' in ('mobile', 'web') as is_digital,
    (CURRENT_DATE - {{ format_date("(digital_engagement::jsonb)->>'last_login_date'") }}::date)::integer as days_since_last_login

from {{ source('silver', 'stg_customers') }}
where digital_engagement is not null