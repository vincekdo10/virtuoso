{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with source_data as (
    select
        consumerid,
        interface_id,
        agent_interface_id,
        extended_data
    from {{ source('consumer', 'stream_consumer_extension') }}
    where metadata$action = 'INSERT'
    
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_extension') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

transformed as (
    select
        consumerid,
        interface_id,
        agent_interface_id,
        extended_data,
        current_timestamp() as updated_timestamp,
        current_timestamp() as inserted_timestamp,
        'stream_consumer_extension' as source_stream
    from source_data
)

select * from transformed 