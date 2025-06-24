{% macro log_consumer_merge_results(stream_name, records_affected) %}
  {% if execute %}
    {% set log_sql %}
      call sharedservices.log.query_log(
        '',
        '{{ invocation_id }}',
        '',
        '',
        'CONSUMER_MASTER_MERGE_DBT',
        '{{ stream_name }}',
        '',
        'SHAREDSERVICES',
        '{{ records_affected }}',
        ''
      )
    {% endset %}
    
    {% do run_query(log_sql) %}
    {{ log("Logged " ~ records_affected ~ " records for " ~ stream_name, info=true) }}
  {% endif %}
{% endmacro %}

{% macro get_consumer_merge_summary() %}
  {% if execute %}
    {% set summary_sql %}
      select 
        'CONSUMER_MASTER_MERGE_DBT' as procedure_name,
        current_timestamp() as execution_time,
        '{{ invocation_id }}' as dbt_invocation_id,
        count(*) as total_records_processed
      from {{ ref('dim_consumer_master') }}
      where updated_timestamp >= current_date()
    {% endset %}
    
    {% set results = run_query(summary_sql) %}
    {% if results %}
      {% for row in results %}
        {{ log("Consumer merge summary - Total records: " ~ row[3], info=true) }}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro consumer_merge_error_handler(error_message, sqlcode) %}
  {% if execute %}
    {% set error_log_sql %}
      call sharedservices.log.query_log(
        '',
        '',
        'Error during dbt consumer merge execution',
        '{{ sqlcode }}',
        'CONSUMER_MASTER_MERGE_DBT',
        'CONSUMER_MASTER_MERGE',
        '',
        'SHAREDSERVICES',
        'None',
        ''
      )
    {% endset %}
    
    {% do run_query(error_log_sql) %}
    {{ log("Error logged: " ~ error_message, info=true) }}
  {% endif %}
{% endmacro %} 