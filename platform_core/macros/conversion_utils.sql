{% macro kw_to_mw(column_name, precision=2) %}

    round( {{ column_name }} / 1000, {{ precision }} )

{% endmacro %}
