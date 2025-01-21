{%- macro gen_sk(field_list, max_varchar_string_length=64) -%}

{%- if var('surrogate_key_treat_nulls_as_empty_strings', False) -%}
    {%- set default_null_value = "" -%}
{%- else -%}
    {%- set default_null_value = '_dbt_utils_surrogate_key_null_' -%}
{%- endif -%}

{%- set fields = [] -%}

{%- for field in field_list -%}
    {%- if max_varchar_string_length == null -%}
        {%- do fields.append(
            "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '" ~ default_null_value  ~"')"
        ) -%}
    {%- else -%}
        {%- do fields.append(
            "coalesce(cast(" ~ field ~ " as VARCHAR(" ~ max_varchar_string_length ~ ")), '" ~ default_null_value  ~"')"
        ) -%}
    {%- endif -%}

    {%- if not loop.last %}
        {%- do fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

{%- set concatenated_string = dbt.concat(fields) -%}

lower(convert(varchar(32), hashbytes('md5', coalesce(convert(varchar(8000), {{ concatenated_string }}), '')), 2))
{# {{ dbt.hash(concatenated_string) }} #}

{%- endmacro -%}