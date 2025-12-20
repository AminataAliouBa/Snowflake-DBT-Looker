{{
  config(
    materialized='incremental',
    unique_key=['categorie', 'date_commande', 'segment'],
    tags=['bronze', 'raw'],
    on_schema_change='fail'
  )
}}

-- Chargement depuis le stage Azure
SELECT
    $1::VARCHAR(100) AS categorie,
    $2::DATE AS date_commande,
    $3::VARCHAR(50) AS segment,
    $4::NUMBER(15,2) AS objectif_vente,
    CURRENT_TIMESTAMP() AS _loaded_at,
    METADATA$FILENAME AS _source_file,
    METADATA$FILE_ROW_NUMBER AS _file_row_number
FROM @coms_data.PUBLIC.azure_stage/Objectifs_ventes.csv
(FILE_FORMAT => 'coms_data.BRONZE.csv_format')

{% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}