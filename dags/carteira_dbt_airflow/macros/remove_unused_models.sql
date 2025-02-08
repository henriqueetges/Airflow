/* THIS MACRO IS INTENDED TO BE USED TO CLEAN WHICHEVER TABLES HAVE BEEN DELETED AS MODELS FROM DBT, AS TO NOT KEEP THE DATABASE CROWDED */
{% macro drop_unused_tables() %}
    {% set existing_tables_query %}
        SELECT table_name
        FROM inv.information_schema.tables
        WHERE table_schema = 'dbt'
    {% endset %}

    {% set result = run_query(existing_tables_query) %}
    {% if result and result|length > 0 %}
        {% set existing_tables_query = result.columns[0].values() %}
    {% else %}
        {% set existing_tables = [] %}
    {% endif %}

    {% set dbt_models_query %}
        SELECT 
            table_name 
        FROM inv.information_schema.tables 
        WHERE table_schema = 'dbt' 
        and table_name like 'dbt_%'
    {% endset %}

    {% set dbt_models_result = run_query(dbt_models_query) %}

    {% if dbt_models_result and dbt_models_result|length > 0 %}
        {% set dbt_models = dbt_models_result.columns[0].values() %}
    {% else %}
        {% set dbt_models = [] %}
    {% endif %}

    {% set tables_dropped = false %}

/* need to fix this, for some reason it is not dropping tables and not logging the dropped/udropped correctly */
    {% for table in existing_tables %}
        {% if table not in dbt_models %}
            {% do log("table name: " ~ table, info=True) %}
            {% do run_query("DROP TABLE IF EXISTS inv.dbt." ~ table) %}
            {% do log("Dropped unused table: " ~ table, info=True) %}
            {% set tables_dropped = true %}
        {% endif %}
    {% endfor %}
    
    {% if not tables_dropped %}
        {% do log("No tables to be dropped", info=True) %}
    {% endif %}
{% endmacro %}