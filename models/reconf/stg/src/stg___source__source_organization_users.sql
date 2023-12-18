/*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(2a349b43d0a9efe7dbc1fee97dd47f04)
*/

{{
  config(
    unique_key = q("___hash"),
    merge_update_columns = [q("___hash")],
    on_schema_change = "append_new_columns",
    full_refresh = var('rc_src_refresh', False)
  )
}}
-- Loading incremental helpers for stg___source__source_organization_users
{%- set max_loaded_at = "" -%}
{% if is_incremental() %}
{%- call statement("max_loaded_at_stmt", fetch_result=True) -%}
SELECT CAST(MAX({{ q("___loaded_at") }}) AS {{ t('string') }}) FROM {{ this }}
{%- endcall -%}
{%- set max_loaded_at = load_result("max_loaded_at_stmt")["data"][0][0] -%}
{% endif %}
-- End of incremental helpers
WITH
  base AS (
    WITH ___pre AS (
      SELECT
        -- Column: ___hash
        {{ hash_from_cols([
             "_id",
             "updatedat",
             "terms",
             "__hevo__ingested_at",
             "deletedat",
             "__hevo__database_name",
             "userid",
             "__hevo_id",
             "__hevo__source_modified_at",
             "__v",
             "__hevo__marked_deleted",
             "organizationid",
             "createdat",
             "__hevo__loaded_at"
           ], "rc1m99m23") +
           " AS " + q("___hash") }},
        -- Column: ___as_of
        {{ "CAST(" +
               "rc1m99m23.createdat" +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___as_of") }},
        -- Column: ___loaded_at
        {{ "CAST(" +
               run_start_ts() +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___loaded_at") }},
        -- Column: ___source_loaded_at
        {{ "CAST(" +
               run_start_ts() +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___source_loaded_at") }},
        -- Column: ___is_deleted
        {{ "FALSE" +
           " AS " + q("___is_deleted") }},
        -- Column: ___source_id
        {{ "CAST(" + "rc1m99m23._id" + " AS " + t("string") + ")" +
           " AS " + q("___source_id") }},
        -- Column: _id
        {{ "rc1m99m23._id" +
           " AS " + q("_id") }},
        -- Column: updatedat
        {{ "rc1m99m23.updatedat" +
           " AS " + q("updatedat") }},
        -- Column: terms
        {{ "rc1m99m23.terms" +
           " AS " + q("terms") }},
        -- Column: __hevo__ingested_at
        {{ "rc1m99m23.__hevo__ingested_at" +
           " AS " + q("__hevo__ingested_at") }},
        -- Column: deletedat
        {{ "rc1m99m23.deletedat" +
           " AS " + q("deletedat") }},
        -- Column: __hevo__database_name
        {{ "rc1m99m23.__hevo__database_name" +
           " AS " + q("__hevo__database_name") }},
        -- Column: userid
        {{ "rc1m99m23.userid" +
           " AS " + q("userid") }},
        -- Column: __hevo_id
        {{ "rc1m99m23.__hevo_id" +
           " AS " + q("__hevo_id") }},
        -- Column: __hevo__source_modified_at
        {{ "rc1m99m23.__hevo__source_modified_at" +
           " AS " + q("__hevo__source_modified_at") }},
        -- Column: __v
        {{ "rc1m99m23.__v" +
           " AS " + q("__v") }},
        -- Column: __hevo__marked_deleted
        {{ "rc1m99m23.__hevo__marked_deleted" +
           " AS " + q("__hevo__marked_deleted") }},
        -- Column: organizationid
        {{ "rc1m99m23.organizationid" +
           " AS " + q("organizationid") }},
        -- Column: createdat
        {{ "rc1m99m23.createdat" +
           " AS " + q("createdat") }},
        -- Column: __hevo__loaded_at
        {{ "rc1m99m23.__hevo__loaded_at" +
           " AS " + q("__hevo__loaded_at") }}
      FROM {{ source("source", "source_organization_users") }} AS rc1m99m23
      WHERE {{
        "( " + if_incr("( " + run_start_ts() + " > " + "CAST(" +
            string_literal(max_loaded_at) +
            " AS " + t("timestamp") +
          ")" + " )", "1 = 1") +
        " AND " + "( " + run_start_ts() + " <= " + run_start_ts() + " )" + " )"
      }}
    )
    ,___rn_added AS (
      SELECT
        {{ q("___hash") }}
        , {{ q("___as_of") }}
        , {{ q("___loaded_at") }}
        , {{ q("___source_loaded_at") }}
        , {{ q("___is_deleted") }}
        , {{ q("___source_id") }}
        , {{ q("_id") }}
        , {{ q("updatedat") }}
        , {{ q("terms") }}
        , {{ q("__hevo__ingested_at") }}
        , {{ q("deletedat") }}
        , {{ q("__hevo__database_name") }}
        , {{ q("userid") }}
        , {{ q("__hevo_id") }}
        , {{ q("__hevo__source_modified_at") }}
        , {{ q("__v") }}
        , {{ q("__hevo__marked_deleted") }}
        , {{ q("organizationid") }}
        , {{ q("createdat") }}
        , {{ q("__hevo__loaded_at") }}
        , ROW_NUMBER() OVER (PARTITION BY {{ q("___hash") }} ORDER BY {{ q("___as_of") }} ASC) AS {{ q("___rn") }}
      FROM ___pre
    )
    SELECT
      {{ q("___hash") }}
      , {{ q("___as_of") }}
      , {{ q("___loaded_at") }}
      , {{ q("___source_loaded_at") }}
      , {{ q("___is_deleted") }}
      , {{ q("___source_id") }}
      , {{ q("_id") }}
      , {{ q("updatedat") }}
      , {{ q("terms") }}
      , {{ q("__hevo__ingested_at") }}
      , {{ q("deletedat") }}
      , {{ q("__hevo__database_name") }}
      , {{ q("userid") }}
      , {{ q("__hevo_id") }}
      , {{ q("__hevo__source_modified_at") }}
      , {{ q("__v") }}
      , {{ q("__hevo__marked_deleted") }}
      , {{ q("organizationid") }}
      , {{ q("createdat") }}
      , {{ q("__hevo__loaded_at") }}
    FROM ___rn_added
    WHERE {{ q("___rn") }} = 1
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base

