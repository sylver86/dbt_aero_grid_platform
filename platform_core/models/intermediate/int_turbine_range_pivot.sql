

                                                                                                    /* 1. Definiamo la lista delle soglie */
{% set power_thresholds = [100, 500, 1000, 2000] %}

with performance as (
    select * from {{ ref('int_turbine_performance_check') }}
),

final as (
    select
        turbine_id,
        /* 2. Ciclo dinamico sulle soglie */
        {% for thresh in power_thresholds %}
            sum(case when power_output_kw >= {{ thresh }} then 1 else 0 end) as readings_above_{{ thresh }}_kw
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from performance
    group by 1
)

select * from final
