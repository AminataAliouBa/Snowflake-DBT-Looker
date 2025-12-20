-- ============================================================================
-- Setup Snowflake - Stage et Format uniquement
-- Les tables seront créées par DBT
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Créer la base de données
USE DATABASE coms_data;

-- Créer le schéma Bronze pour le stage
CREATE SCHEMA IF NOT EXISTS BRONZE;
USE SCHEMA BRONZE;

-- Créer le warehouse

USE WAREHOUSE COMPUTE_WH;

-- Créer le format CSV
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('NULL', 'null', '', 'N/A')
    ENCODING = 'UTF8';

list @azure_stage;

-- Tester le chargement (optionnel - pour vérifier le format)
SELECT
  $1 AS id_ligne,
  $2 AS id_commande,
  $3 AS date_commande
FROM @azure_stage/Hypermarche.csv
(FILE_FORMAT => 'csv_format')
LIMIT 10;

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

select count(distinct id_commande) from bronze.bronze_hypermarche;
select count(distinct client_key) from analytics.kpi_analyses_client;
select count(distinct [annee,
        mois_numero,
        mois_nom,
        trimestre,
        annee_mois,
        segment,
        region,
        pays,
        zone_geographique,
        categorie,
        sous_categorie]) from analytics.kpi_perfs_mensuelle;