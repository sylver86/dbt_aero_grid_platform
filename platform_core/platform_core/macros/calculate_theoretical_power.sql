{% macro elab_power_theoretical(wind_column)%}

    ROUND(POWER(CAST({{ wind_column }} AS float64), 3) * 0.5, 2)

{% endmacro %}
