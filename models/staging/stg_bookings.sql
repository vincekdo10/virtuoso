{{ config(
    materialized='dynamic_table',
    on_configuration_change='apply',
    target_lag='downstream',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with source_data as (
    select
        data_source_id,
        sc_booking_id,
        agency_id,
        agency_branch_id,
        booking_id,
        supplier_id,
        invoice_number,
        booking_issue_date,
        travel_start_date,
        travel_end_date,
        booking_type_id,
        booking_subtype_id,
        origin_airport_id,
        destination_airport_id,
        booking_attribute_id,
        consumer_id,
        primary_advisor_id,
        marketing_advisor_id,
        currency_code,
        total_booking_amount,
        is_deleted,
        exclude_booking,
        void_transaction
    from {{ source('published', 'booking_master') }}
),

transformed as (
    select
        data_source_id,
        sc_booking_id,
        agency_id,
        agency_branch_id,
        booking_id,
        supplier_id,
        invoice_number,
        booking_issue_date,
        travel_start_date,
        travel_end_date,
        booking_type_id,
        booking_subtype_id,
        origin_airport_id,
        destination_airport_id,
        booking_attribute_id,
        consumer_id,
        primary_advisor_id,
        marketing_advisor_id,
        currency_code,
        total_booking_amount,
        is_deleted,
        exclude_booking,
        void_transaction,
        -- Add computed columns for staging
        case 
            when is_deleted = false 
                 and exclude_booking = false 
                 and void_transaction = false 
            then true 
            else false 
        end as is_valid_booking,
        case 
            when total_booking_amount >= 1000 then 'high_value'
            when total_booking_amount >= 500 then 'medium_value'
            when total_booking_amount > 0 then 'low_value'
            else 'zero_value'
        end as booking_value_tier,
        datediff('day', travel_start_date, travel_end_date) as travel_duration_days
    from source_data
)

select * from transformed 