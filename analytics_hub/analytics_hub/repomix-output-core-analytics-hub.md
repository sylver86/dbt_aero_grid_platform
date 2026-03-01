This file is a merged representation of the entire codebase, combined into a single document by Repomix.

<file_summary>
This section contains a summary of this file.

<purpose>
This file contains a packed representation of the entire repository's contents.
It is designed to be easily consumable by AI systems for analysis, code review,
or other automated processes.
</purpose>

<file_format>
The content is organized as follows:
1. This summary section
2. Repository information
3. Directory structure
4. Repository files (if enabled)
5. Multiple file entries, each consisting of:
  - File path as an attribute
  - Full contents of the file
</file_format>

<usage_guidelines>
- This file should be treated as read-only. Any changes should be made to the
  original repository files, not this packed version.
- When processing this file, use the file path to distinguish
  between different files in the repository.
- Be aware that this file may contain sensitive information. Handle it with
  the same level of security as you would the original repository.
</usage_guidelines>

<notes>
- Some files may have been excluded based on .gitignore rules and Repomix's configuration
- Binary files are not included in this packed representation. Please refer to the Repository Structure section for a complete list of file paths, including binary files
- Files matching patterns in .gitignore are excluded
- Files matching default ignore patterns are excluded
- Files are sorted by Git change count (files with more changes are at the bottom)
</notes>

</file_summary>

<directory_structure>
analyses/
  .gitkeep
macros/
  .gitkeep
  generate_schema_name.sql
models/
  marts/
    test_core_link.sql
seeds/
  .gitkeep
snapshots/
  .gitkeep
tests/
  .gitkeep
.gitignore
dbt_project.yml
package-lock.yml
packages.yml
README.md
</directory_structure>

<files>
This section contains the contents of the repository's files.

<file path="analyses/.gitkeep">

</file>

<file path="macros/.gitkeep">

</file>

<file path="macros/generate_schema_name.sql">
-- macros/generate_schema_name.sql di ANALYTICS_HUB
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {#
        Modelli di platform_core: punta ai dataset di produzione

        node.package_name è una proprietà del singolo modello che dbt sta compilando in quel momento.
        Indica esattamente "questo modello appartiene a quale progetto/package?".

        Quindi se dbt sta compilando fct_turbine_telemetry, node.package_name sarà 'platform_core'.
        Se sta compilando test_core_link, sarà 'analytics_hub'

        project_name è una variabile globale che ti dice "da quale progetto ho lanciato il comando dbt run?".
        Se viene eseguito dbt run dalla cartella di analytics_hub, project_name sarà sempre 'analytics_hub',
        indipendentemente da quale modello sta compilando.

    #}
    {%- if node.package_name == 'platform_core' -%}
        {%- if custom_schema_name is not none -%}
            prod_aero_grid_platform_{{ custom_schema_name | trim }}
        {%- else -%}
            prod_aero_grid_platform
        {%- endif -%}

    {# Modelli di analytics_hub: comportamento standard #}
    {%- elif custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
</file>

<file path="models/marts/test_core_link.sql">
{{ config(materialized='view') }}

select * from {{ ref('platform_core', 'fct_turbine_telemetry') }}                                   /* Puntiamo al progetto platform_core e prendiamo la tabella fct_turbine_telemetrys */
limit 10
</file>

<file path="seeds/.gitkeep">

</file>

<file path="snapshots/.gitkeep">

</file>

<file path="tests/.gitkeep">

</file>

<file path=".gitignore">
target/
dbt_packages/
logs/
</file>

<file path="dbt_project.yml">
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'analytics_hub'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'aero_grid_analytics'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  analytics_hub:
    marts:
      +materialized: table
      +schema: marts
</file>

<file path="package-lock.yml">
packages:
  - local: ../../platform_core/platform_core
    name: platform_core
  - name: codegen
    package: dbt-labs/codegen
    version: 0.14.0
  - name: dbt_utils
    package: dbt-labs/dbt_utils
    version: 1.3.3
sha1_hash: 7db87d427e98c01ae247c8bdc18109501e38afe1
</file>

<file path="packages.yml">
packages:
  - local: ../../platform_core/platform_core
</file>

<file path="README.md">
Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
</file>

</files>
