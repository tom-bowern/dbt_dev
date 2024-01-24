
      
  
    

  create  table
    "analytics"."loads_stg"."stg___source__source_users"
    
    
    
  as (
    /*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(076b93ed67459ffe7e3c5fc92623463c)
*/


-- Loading incremental helpers for stg___source__source_users
-- End of incremental helpers
WITH
  base AS (
    WITH ___pre AS (
      SELECT
        -- Column: ___hash
        SHA2('unverifiedemail' || '|' || CAST(rcbh3mv.unverifiedemail AS TEXT)|| '||' || 'applicationids' || '|' || CAST(rcbh3mv.applicationids AS TEXT)|| '||' || 'applicatonids' || '|' || CAST(rcbh3mv.applicatonids AS TEXT)|| '||' || 'email' || '|' || CAST(rcbh3mv.email AS TEXT)|| '||' || '__hevo__marked_deleted' || '|' || CAST(rcbh3mv.__hevo__marked_deleted AS TEXT)|| '||' || 'visitedat' || '|' || CAST(rcbh3mv.visitedat AS TEXT)|| '||' || '__hevo__ingested_at' || '|' || CAST(rcbh3mv.__hevo__ingested_at AS TEXT)|| '||' || 'createdat' || '|' || CAST(rcbh3mv.createdat AS TEXT)|| '||' || 'verifiedat' || '|' || CAST(rcbh3mv.verifiedat AS TEXT)|| '||' || 'deleteat' || '|' || CAST(rcbh3mv.deleteat AS TEXT)|| '||' || 'name' || '|' || CAST(rcbh3mv.name AS TEXT)|| '||' || 'externalid' || '|' || CAST(rcbh3mv.externalid AS TEXT)|| '||' || 'wallets' || '|' || CAST(rcbh3mv.wallets AS TEXT)|| '||' || 'campaigns' || '|' || CAST(rcbh3mv.campaigns AS TEXT)|| '||' || '__hevo__database_name' || '|' || CAST(rcbh3mv.__hevo__database_name AS TEXT)|| '||' || '__hevo__loaded_at' || '|' || CAST(rcbh3mv.__hevo__loaded_at AS TEXT)|| '||' || '__hevo__source_modified_at' || '|' || CAST(rcbh3mv.__hevo__source_modified_at AS TEXT)|| '||' || 'settings' || '|' || CAST(rcbh3mv.settings AS TEXT)|| '||' || 'updatedat' || '|' || CAST(rcbh3mv.updatedat AS TEXT)|| '||' || 'partners' || '|' || CAST(rcbh3mv.partners AS TEXT)|| '||' || 'assets' || '|' || CAST(rcbh3mv.assets AS TEXT)|| '||' || '__hevo_id' || '|' || CAST(rcbh3mv.__hevo_id AS TEXT)|| '||' || '_id' || '|' || CAST(rcbh3mv._id AS TEXT)|| '||' || '__v' || '|' || CAST(rcbh3mv.__v AS TEXT), 256)
 AS "___hash",
        -- Column: ___as_of
        CAST(rcbh3mv.createdat AS timestamp) AS "___as_of",
        -- Column: ___loaded_at
        CAST(CAST('2023-12-18T10:47:17.756177+00:00' AS timestamp) AS timestamp) AS "___loaded_at",
        -- Column: ___source_loaded_at
        CAST(CAST('2023-12-18T10:47:17.756177+00:00' AS timestamp) AS timestamp) AS "___source_loaded_at",
        -- Column: ___is_deleted
        FALSE AS "___is_deleted",
        -- Column: ___source_id
        CAST(rcbh3mv._id AS TEXT) AS "___source_id",
        -- Column: unverifiedemail
        rcbh3mv.unverifiedemail AS "unverifiedemail",
        -- Column: applicationids
        rcbh3mv.applicationids AS "applicationids",
        -- Column: applicatonids
        rcbh3mv.applicatonids AS "applicatonids",
        -- Column: email
        rcbh3mv.email AS "email",
        -- Column: __hevo__marked_deleted
        rcbh3mv.__hevo__marked_deleted AS "__hevo__marked_deleted",
        -- Column: visitedat
        rcbh3mv.visitedat AS "visitedat",
        -- Column: __hevo__ingested_at
        rcbh3mv.__hevo__ingested_at AS "__hevo__ingested_at",
        -- Column: createdat
        rcbh3mv.createdat AS "createdat",
        -- Column: verifiedat
        rcbh3mv.verifiedat AS "verifiedat",
        -- Column: deleteat
        rcbh3mv.deleteat AS "deleteat",
        -- Column: name
        rcbh3mv.name AS "name",
        -- Column: externalid
        rcbh3mv.externalid AS "externalid",
        -- Column: wallets
        rcbh3mv.wallets AS "wallets",
        -- Column: campaigns
        rcbh3mv.campaigns AS "campaigns",
        -- Column: __hevo__database_name
        rcbh3mv.__hevo__database_name AS "__hevo__database_name",
        -- Column: __hevo__loaded_at
        rcbh3mv.__hevo__loaded_at AS "__hevo__loaded_at",
        -- Column: __hevo__source_modified_at
        rcbh3mv.__hevo__source_modified_at AS "__hevo__source_modified_at",
        -- Column: settings
        rcbh3mv.settings AS "settings",
        -- Column: updatedat
        rcbh3mv.updatedat AS "updatedat",
        -- Column: partners
        rcbh3mv.partners AS "partners",
        -- Column: assets
        rcbh3mv.assets AS "assets",
        -- Column: __hevo_id
        rcbh3mv.__hevo_id AS "__hevo_id",
        -- Column: _id
        rcbh3mv._id AS "_id",
        -- Column: __v
        rcbh3mv.__v AS "__v"
      FROM "analytics"."source"."source_users" AS rcbh3mv
      WHERE ( 1 = 1 AND ( CAST('2023-12-18T10:47:17.756177+00:00' AS timestamp) <= CAST('2023-12-18T10:47:17.756177+00:00' AS timestamp) ) )
    )
    ,___rn_added AS (
      SELECT
        "___hash"
        , "___as_of"
        , "___loaded_at"
        , "___source_loaded_at"
        , "___is_deleted"
        , "___source_id"
        , "unverifiedemail"
        , "applicationids"
        , "applicatonids"
        , "email"
        , "__hevo__marked_deleted"
        , "visitedat"
        , "__hevo__ingested_at"
        , "createdat"
        , "verifiedat"
        , "deleteat"
        , "name"
        , "externalid"
        , "wallets"
        , "campaigns"
        , "__hevo__database_name"
        , "__hevo__loaded_at"
        , "__hevo__source_modified_at"
        , "settings"
        , "updatedat"
        , "partners"
        , "assets"
        , "__hevo_id"
        , "_id"
        , "__v"
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
      , "unverifiedemail"
      , "applicationids"
      , "applicatonids"
      , "email"
      , "__hevo__marked_deleted"
      , "visitedat"
      , "__hevo__ingested_at"
      , "createdat"
      , "verifiedat"
      , "deleteat"
      , "name"
      , "externalid"
      , "wallets"
      , "campaigns"
      , "__hevo__database_name"
      , "__hevo__loaded_at"
      , "__hevo__source_modified_at"
      , "settings"
      , "updatedat"
      , "partners"
      , "assets"
      , "__hevo_id"
      , "_id"
      , "__v"
    FROM ___rn_added
    WHERE "___rn" = 1
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base
  );
  
  