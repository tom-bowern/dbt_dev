{{
    config(
        unique_key='organization_id',
        schema='staging',
        materialized='table',
        tags=['auth_service','organization']
    )
}}

WITH 
historic AS (
    SELECT *
    FROM {{ source('source','dbt_seeds_organizations') }}
),
created AS (
    SELECT _id AS organization_id,
           hubspottrackingcode AS hubspot_tracking_code,
           COALESCE(NAME, productname) AS organization_name,
           productname AS product_name,
           employees AS employee_bucket,
           website,
           planstouserpm AS plans_to_use_rpm,
           "references" AS reference,
           createdat AS created_at,
           updatedat AS updated_at,
           ROW_NUMBER() OVER (PARTITION BY hubspot_tracking_code ORDER BY updated_at DESC) AS row_number
    FROM {{ source('services','auth_service_v1_organization_created') }} 
),
created_dedup AS (
    SELECT *
    FROM created
    WHERE row_number = 1
),
combined AS (
    SELECT organization_id,
           hubspot_id,
           NULL AS hubspot_tracking_code,
           organization_name,
           product_name,
           employee_bucket,
           website,
           plans_to_use_rpm,
           reference,
           created_at::timestamp,
           updated_at::timestamp
    FROM historic
    UNION ALL
    SELECT organization_id,
           NULL AS hubspot_id,
           hubspot_tracking_code,
           organization_name,
           product_name,
           employee_bucket,
           website,
           plans_to_use_rpm,
           reference,
           created_at,
           updated_at
    FROM created_dedup
),
latest_organization_record AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY updated_at DESC) AS row_number
    FROM combined
)
SELECT latest_organization_record.organization_id
     , latest_organization_record.hubspot_id
     , latest_organization_record.hubspot_tracking_code
     , latest_organization_record.organization_name
     , latest_organization_record.product_name
     , latest_organization_record.employee_bucket
     , latest_organization_record.website
     , latest_organization_record.plans_to_use_rpm
     , latest_organization_record.reference
     , latest_organization_record.created_at
     , latest_organization_record.updated_at
FROM latest_organization_record
WHERE row_number = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11