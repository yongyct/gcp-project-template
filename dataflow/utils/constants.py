# Constants
## dataflow config file properties
PROGRAM_ARGS_CONF = 'program_args'
PIPELINE_ARGS_CONF = 'pipeline_args'
INPUT_CONF = 'input'
OUTPUT_CONF = 'output'
RUNNER_CONF = 'runner'
PROJECT_ID_CONF = 'project'
STAGING_LOC_CONF = 'staging_location'
TEMP_LOC_CONF = 'temp_location'
REJECT_CONF = 'reject'

## ddl_generator config file properties
ETL_CONFIG_PATH_CONF = 'etl_config_path'
TYPE_MAPPING_PATH_CONF = 'type_mapping_path'
BQ_PROJECT_CONF = 'bigquery_project'
BQ_DATASET_CONF = 'bigquery_dataset'
METADATA_FIELDS_CONF = 'metadata_fields'

## gcs etl config file properties
HEADER_CONF = 'header'

## gcs ingestion config json keys
SCHEMA_PATH_KEY = 'schema'

## schema json keys
NAME_KEY = 'name'
FIELDS_KEY = 'fields'
TYPE_KEY = 'type'
LENGTH_KEY = 'length'
IS_PK_KEY = 'primary_key'
NULLABLE_KEY = 'nullable'

## validation constants
VALID_ENVS = ['dev', 'sit', 'uat', 'prd']
