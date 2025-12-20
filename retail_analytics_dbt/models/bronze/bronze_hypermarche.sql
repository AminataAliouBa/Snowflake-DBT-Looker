{{
  config(
    materialized='incremental',
    unique_key='id_ligne',
    tags=['bronze', 'raw'],
    on_schema_change='fail'
  )
}}

-- Chargement depuis le stage Azure via table externe Snowflake
SELECT
    $1::VARCHAR(50) AS id_ligne,
    $2::VARCHAR(50) AS id_commande,
    $3::DATE AS date_commande,
    $4::DATE AS date_expedition,
    $5::VARCHAR(50) AS mode_expedition,
    $6::VARCHAR(50) AS id_client,
    $7::VARCHAR(200) AS nom_client,
    $8::VARCHAR(50) AS segment,
    $9::VARCHAR(100) AS ville,
    $10::VARCHAR(100) AS region,
    $11::VARCHAR(100) AS pays,
    $12::VARCHAR(100) AS zone_geographique,
    $13::VARCHAR(50) AS id_produit,
    $14::VARCHAR(100) AS categorie,
    $15::VARCHAR(100) AS sous_categorie,
    $16::VARCHAR(300) AS nom_produit,
    $17::NUMBER(15,2) AS montant_ventes,
    $18::NUMBER(10,2) AS quantite,
    $19::NUMBER(5,4) AS remise,
    $20::NUMBER(15,2) AS profit,
    CURRENT_TIMESTAMP() AS _loaded_at,
    METADATA$FILENAME AS _source_file,
    METADATA$FILE_ROW_NUMBER AS _file_row_number
FROM @coms_data.PUBLIC.azure_stage/Hypermarche.csv
(FILE_FORMAT => 'coms_data.BRONZE.csv_format')

{% if is_incremental() %}
    -- En mode incrémental, ne charger que les nouvelles lignes
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}