import json
import yaml
import argparse
import time
from pathlib import Path

from google.cloud import storage

from ..exceptions.conf_error import InvalidConfigError, InvalidSchemaError
from .conf_utils import get_program_conf
from .gcs_utils import get_gcs_json_as_dict, get_gcs_blob_list
from .constants import ETL_CONFIG_PATH_CONF, TYPE_MAPPING_PATH_CONF, PROJECT_ID_CONF,\
BQ_DATASET_CONF, METADATA_FIELDS_CONF, SCHEMA_PATH_KEY, NAME_KEY, FIELDS_KEY,\
TYPE_KEY, LENGTH_KEY, IS_PK_KEY, NULLABLE_KEY, VALID_ENVS


def get_field_definition_list(field_dict_list, type_mapping_dict, metadata_fields):
    """Parses and returns the list of field definitions to form the DDL for a schema"""
    field_definition_list = []
    # Add metadata fields
    for metadata_field in metadata_fields:
        field_definition = [metadata_field[NAME_KEY], metadata_field[TYPE_KEY]]
        field_definition_list.append(' '.join(field_definition))
    for field in field_dict_list:
        # Validations
        if (
            NAME_KEY not in field
            or TYPE_KEY not in field
            or LENGTH_KEY not in field
            or IS_PK_KEY not in field
            or NULLABLE_KEY not in field
        ):
            raise InvalidSchemaError('Missing schema keys for field defined: {}'.format(field))
        # Validations
        if field[TYPE_KEY] not in type_mapping_dict:
            raise InvalidSchemaError('Data type mapping not available: {}'.format(field))

        field_type = type_mapping_dict[field[TYPE_KEY]]
        field_length = field[LENGTH_KEY]

        if not field[NULLABLE_KEY]:
            field_definition = [field[NAME_KEY], field_type, 'NOT NULL']
        else:
            field_definition = [field[NAME_KEY], field_type]

        field_definition_list.append(' '.join(field_definition))

    return field_definition_list


def get_ddl_list(env):
    """Get list of ddls based on environment where code is run"""
    program_conf = get_program_conf(env=env)

    type_mapping_dict = {}
    with open(program_conf[TYPE_MAPPING_PATH_CONF], 'r') as type_mapping_file:
        type_mapping_dict = json.loads(type_mapping_file.read())

    ddl_list = []
    gcs_client = storage.Client()
    etl_config_dict = get_gcs_json_as_dict(program_conf[ETL_CONFIG_PATH_CONF], gcs_client)
    schema_file_list = get_gcs_blob_list(etl_config_dict[SCHEMA_PATH_KEY], gcs_client)
    for schema_file_url in schema_file_list:
        schema_dict = get_gcs_json_as_dict(schema_file_url, gcs_client)
        table_name = schema_dict[NAME_KEY]

        field_definition_list = get_field_definition_list(
            field_dict_list=schema_dict[FIELDS_KEY], 
            type_mapping_dict=type_mapping_dict,
            metadata_fields=program_conf[METADATA_FIELDS_CONF]
        )

        ddl = 'CREATE TABLE IF NOT EXISTS `{}.{}.{}` ( {} )'.format(
                program_conf[PROJECT_ID_CONF], 
                program_conf[BQ_DATASET_CONF], 
                table_name, 
                ', '.join(field_definition_list)
            )
        ddl_list.append(ddl)
    return ddl_list
