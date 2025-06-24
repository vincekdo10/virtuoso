{{ config(
    materialized='incremental',
    unique_key='consumerid',
    on_schema_change='fail'
) }}

with source_data as (
    select *
    from {{ source('consumer', 'stream_consumer') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

transformed as (
    select
        consumerid,
        sourcesystemtimestamp,
        is_active,
        first_name,
        last_name,
        membernumber,
        branch_member_number,
        prefix,
        suffix,
        salutation,
        account_name,
        address_line_1,
        address_line_2,
        aptsuite,
        city,
        state,
        zip,
        country,
        secondary_address_line_1,
        secondary_address_line_2,
        secondary_aptsuite,
        secondary_city,
        secondary_state,
        secondary_zip,
        secondary_country,
        secondary_address_from_month,
        secondary_address_to_month,
        primary_email,
        phone_number,
        business_phone,
        cell_phone,
        account_id,
        preferred_language,
        fax_phone,
        sourcesystem,
        primary_agent_id,
        data_source_id,
        create_date,
        isdeleted,
        deleted_timestamp,
        current_timestamp() as processed_at
    from source_data
)

select * from transformed 