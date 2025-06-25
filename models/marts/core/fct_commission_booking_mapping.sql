{{ config(
    materialized='dynamic_table',
    on_configuration_change='apply',
    target_lag='downstream',
    snowflake_warehouse='WH_PIPE_COMMISSION',
    unique_key='commissionid'
) }}

with commission_data as (
    select 
        commissionid,
        scbookingid,
        datasourceid
    from {{ ref('stg_commission_data') }}
),

booking_data as (
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
        is_valid_booking,
        booking_value_tier,
        travel_duration_days
    from {{ ref('stg_bookings') }}
)

select
    cmw.commissionid,
    cmw.scbookingid,
    cmw.datasourceid,
    bm.agency_id,
    bm.agency_branch_id as agencybranchid,
    bm.booking_id,
    bm.supplier_id,
    bm.invoice_number,
    bm.booking_issue_date,
    bm.travel_start_date,
    bm.travel_end_date,
    bm.booking_type_id,
    bm.booking_subtype_id,
    bm.origin_airport_id,
    bm.destination_airport_id,
    bm.booking_attribute_id,
    bm.consumer_id,
    bm.primary_advisor_id,
    bm.marketing_advisor_id,
    bm.currency_code as booking_currencycode,
    bm.agency_branch_id,
    bm.total_booking_amount,
    -- Business logic from original query
    case 
        when bm.booking_id is not null 
             and bm.is_deleted = false 
             and bm.exclude_booking = false 
             and bm.void_transaction = false 
        then 1 
        else 0 
    end as isbookingmapped,
    -- Additional business metrics
    bm.booking_value_tier,
    bm.travel_duration_days,
    bm.is_valid_booking,
    case 
        when bm.booking_id is not null then 'mapped'
        else 'unmapped'
    end as mapping_status
from commission_data cmw
left join booking_data bm
    on cmw.datasourceid = bm.data_source_id
    and cmw.scbookingid = bm.sc_booking_id 