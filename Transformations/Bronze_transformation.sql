-- ============================================================================
-- BRONZE LAYER: RAW DATA INGESTION
-- ============================================================================
-- Ingests CSV files from volume using Auto Loader (incremental processing)
-- Source: /Volumes/LEGACY_INC/bronze/landing/raw/
-- Target: LEGACY_INC.bronze.sales_catalogue (116,294 rows)
-- Schema: Auto-inferred with metadata columns added
-- ============================================================================

-- ============================================================================
-- sales_catalogue: Mixed entity data (products, customers, sales, categories)
-- Type: Streaming Table with Auto Loader
-- Format: CSV with header row
-- Quality: Ensures metadata exists, warns if no entity identifiers present
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE LEGACY_INC.bronze.sales_catalogue
(
  CONSTRAINT valid_source_file EXPECT (source_file IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_ingestion_time EXPECT (ingestion_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_file_timestamp EXPECT (file_timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_entity_data EXPECT (
    prd_id IS NOT NULL OR cst_id IS NOT NULL OR sls_ord_num IS NOT NULL
  )
)
COMMENT 'Raw sales catalogue data ingested from landing zone'
AS SELECT 
  *,
  _metadata.file_path as source_file,
  _metadata.file_modification_time as file_timestamp,
  current_timestamp() as ingestion_timestamp
FROM STREAM(read_files(
  '/Volumes/LEGACY_INC/bronze/landing/raw/',
  format => 'csv',
  header => true
));
