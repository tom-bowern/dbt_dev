/*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(2a349b43d0a9efe7dbc1fee97dd47f04)
*/


-- Loading incremental helpers for stg___source__source_organization_users
-- End of incremental helpers
WITH
  base AS (
    WITH ___pre AS (
      SELECT
        -- Column: ___hash
        SHA2('_id' || '|' || CAST(rc1m99m23._id AS TEXT)|| '||' || 'updatedat' || '|' || CAST(rc1m99m23.updatedat AS TEXT)|| '||' || 'terms' || '|' || CAST(rc1m99m23.terms AS TEXT)|| '||' || '__hevo__ingested_at' || '|' || CAST(rc1m99m23.__hevo__ingested_at AS TEXT)|| '||' || 'deletedat' || '|' || CAST(rc1m99m23.deletedat AS TEXT)|| '||' || '__hevo__database_name' || '|' || CAST(rc1m99m23.__hevo__database_name AS TEXT)|| '||' || 'userid' || '|' || CAST(rc1m99m23.userid AS TEXT)|| '||' || '__hevo_id' || '|' || CAST(rc1m99m23.__hevo_id AS TEXT)|| '||' || '__hevo__source_modified_at' || '|' || CAST(rc1m99m23.__hevo__source_modified_at AS TEXT)|| '||' || '__v' || '|' || CAST(rc1m99m23.__v AS TEXT)|| '||' || '__hevo__marked_deleted' || '|' || CAST(rc1m99m23.__hevo__marked_deleted AS TEXT)|| '||' || 'organizationid' || '|' || CAST(rc1m99m23.organizationid AS TEXT)|| '||' || 'createdat' || '|' || CAST(rc1m99m23.createdat AS TEXT)|| '||' || '__hevo__loaded_at' || '|' || CAST(rc1m99m23.__hevo__loaded_at AS TEXT), 256)
 AS "___hash",
        -- Column: ___as_of
        CAST(rc1m99m23.createdat AS timestamp) AS "___as_of",
        -- Column: ___loaded_at
        CAST(CAST('2023-12-18T10:33:54.322533+00:00' AS timestamp) AS timestamp) AS "___loaded_at",
        -- Column: ___source_loaded_at
        CAST(CAST('2023-12-18T10:33:54.322533+00:00' AS timestamp) AS timestamp) AS "___source_loaded_at",
        -- Column: ___is_deleted
        FALSE AS "___is_deleted",
        -- Column: ___source_id
        CAST(rc1m99m23._id AS TEXT) AS "___source_id",
        -- Column: _id
        rc1m99m23._id AS "_id",
        -- Column: updatedat
        rc1m99m23.updatedat AS "updatedat",
        -- Column: terms
        rc1m99m23.terms AS "terms",
        -- Column: __hevo__ingested_at
        rc1m99m23.__hevo__ingested_at AS "__hevo__ingested_at",
        -- Column: deletedat
        rc1m99m23.deletedat AS "deletedat",
        -- Column: __hevo__database_name
        rc1m99m23.__hevo__database_name AS "__hevo__database_name",
        -- Column: userid
        rc1m99m23.userid AS "userid",
        -- Column: __hevo_id
        rc1m99m23.__hevo_id AS "__hevo_id",
        -- Column: __hevo__source_modified_at
        rc1m99m23.__hevo__source_modified_at AS "__hevo__source_modified_at",
        -- Column: __v
        rc1m99m23.__v AS "__v",
        -- Column: __hevo__marked_deleted
        rc1m99m23.__hevo__marked_deleted AS "__hevo__marked_deleted",
        -- Column: organizationid
        rc1m99m23.organizationid AS "organizationid",
        -- Column: createdat
        rc1m99m23.createdat AS "createdat",
        -- Column: __hevo__loaded_at
        rc1m99m23.__hevo__loaded_at AS "__hevo__loaded_at"
      FROM "analytics"."source"."source_organization_users" AS rc1m99m23
      WHERE ( 1 = 1 AND ( CAST('2023-12-18T10:33:54.322533+00:00' AS timestamp) <= CAST('2023-12-18T10:33:54.322533+00:00' AS timestamp) ) )
    )
    ,___rn_added AS (
      SELECT
        "___hash"
        , "___as_of"
        , "___loaded_at"
        , "___source_loaded_at"
        , "___is_deleted"
        , "___source_id"
        , "_id"
        , "updatedat"
        , "terms"
        , "__hevo__ingested_at"
        , "deletedat"
        , "__hevo__database_name"
        , "userid"
        , "__hevo_id"
        , "__hevo__source_modified_at"
        , "__v"
        , "__hevo__marked_deleted"
        , "organizationid"
        , "createdat"
        , "__hevo__loaded_at"
        , ROW_NUMBER() OVER (PARTITION BY "___hash" ORDER BY "___as_of" ASC) AS "___rn"
      FROM ___pre
    )
    SELECT
      "___hash"
      , "___as_of"
      , "___loaded_at"
      , "___source_loaded_at"
      , "___is_deleted"
      , "___source_id"
      , "_id"
      , "updatedat"
      , "terms"
      , "__hevo__ingested_at"
      , "deletedat"
      , "__hevo__database_name"
      , "userid"
      , "__hevo_id"
      , "__hevo__source_modified_at"
      , "__v"
      , "__hevo__marked_deleted"
      , "organizationid"
      , "createdat"
      , "__hevo__loaded_at"
    FROM ___rn_added
    WHERE "___rn" = 1
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base