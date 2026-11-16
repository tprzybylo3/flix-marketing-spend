{#
  Returns a dbt variable and fails at compile time if its value is not in the allowed list,
  so a typo such as --vars '{tie_break: highst}' stops the run instead of silently using a wrong rule.
  Used by the business-rule variables in the intermediate models, e.g.
  {% set tie_break = allowed_var('tie_break', ['highest', 'lowest']) %}
#}
{% macro allowed_var(name, allowed) %}
    {% set value = var(name) %}
    {% if value not in allowed %}
        {{ exceptions.raise_compiler_error("var '" ~ name ~ "' must be one of " ~ allowed | join(', ') ~ "; got '" ~ value ~ "'") }}
    {% endif %}
    {{ return(value) }}
{% endmacro %}
