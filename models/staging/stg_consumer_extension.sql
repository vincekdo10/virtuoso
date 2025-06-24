{{ config(
    materialized='incremental',
    unique_key='consumerid',
    on_schema_change='fail'
) }}

with source_data as (
    select *
    from {{ source('consumer', 'stream_consumer_extension') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

transformed as (
    select
        consumerid,
        interface_id,
        agent_interface_id,
        extended_data,
        current_timestamp() as processed_at
    from source_data
)

select * from transformed 