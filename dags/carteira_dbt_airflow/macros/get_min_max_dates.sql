{% macro get_min_max_date(table_name, date_column) %}
/* THIS MACRO IS INTENDED SO THAT I CAN GENERATE DATES BETWEEN THE TWO VALUES. I WANT TO BE ABLE TO CALCULATE PORTFOLIO RETURNS DAILY SO I NEED TO HAVE
A RANGE OF DAYS */
    {% set result = run_query(
        "SELECT 
        MIN(" ~ date_column ~ ") AS min_date,
        MAX(" ~ date_column ~ ") AS max_date
        FROM " ~ table_name
    ) %}

    {% if result and result|length>0 %}
        {% set row = result.rows[0] %}
        {% set min_date = row[0] %}
        {% set max_date = row[1] %}
    {% else %}
        {% set min_date = '1900-01-01' %}
        {% set max_date = '2100-01-01' %}
    {% endif %}    
    
    {% do return({'min_date': min_date, 'max_date': max_date}) %}

{% endmacro %}