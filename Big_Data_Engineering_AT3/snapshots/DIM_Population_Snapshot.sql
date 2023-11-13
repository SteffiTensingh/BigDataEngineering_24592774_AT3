-- Dimension Poplulation
{% snapshot DIM_Population%}

{{
    config(
        target_schema='raw',
        strategy='check',
        unique_key='lga_code_2016',
        check_cols=['lga_code_2016','tot_p_m','tot_p_f','tot_p_p']
    )
}}

SELECT 
    lga_code_2016,
    tot_p_m,
    tot_p_f,
    tot_p_p
FROM 
    {{ source('raw', 'table_g01_nsw_lga') }}

{% endsnapshot %}
