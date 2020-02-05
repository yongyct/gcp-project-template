import csv
import logging

from google.cloud import storage

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .exceptions.conf_error import InvalidConfigError
from .utils.conf_utils import get_program_conf
from .utils.gcs_utils import get_gcs_json_as_dict
from .utils.ddl_generator import get_ddl_list
from .utils.constants import INPUT_CONF, OUTPUT_CONF, RUNNER_CONF, PROGRAM_ARGS_CONF, \
PIPELINE_ARGS_CONF, SCHEMA_PATH_KEY, FIELDS_KEY, TYPE_KEY, NULLABLE_KEY, HEADER_CONF, \
ETL_CONFIG_PATH_CONF, REJECT_CONF


# Program Specific Constants
DECIMAL_TYPES = ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE']
IS_VALID_FILE = True


# TODO: Check if possible to make this a common function,
# and if possible to manipulate the IS_VALID_FILE flag thereafter
class ValidateRecord(beam.DoFn):
    """class to perform data/record level validations"""
    def __init__(self, schema, file_path, etl_config):
        self.schema = schema
        self.file_path = file_path
        self.etl_config = etl_config
        self.header_skip = True if etl_config[HEADER_CONF] == 'Y' else False
    def __datatype_check(self, record_attribute, attribute_schema):
        """private method for data type check"""
        if 'INT' in attribute_schema[TYPE_KEY].upper():
            if record_attribute.isdigit():
                return True
        elif attribute_schema[TYPE_KEY].upper() in DECIMAL_TYPES:
            if record_attribute.isdecimal():
                return True
        elif 'CHAR' in attribute_schema[TYPE_KEY].upper() \
            or 'TEXT' in attribute_schema[TYPE_KEY].upper():
            if type(record_attribute) is str:
                return True
        else:
            IS_VALID_FILE = False
            return False
    def __null_check(self, record_attribute, attribute_schema):
        """private method for null check"""
        if attribute_schema[NULLABLE_KEY]:
            return True
        elif record_attribute is not None:
            return True
        else:
            IS_VALID_FILE = False
            return False
    def process(self, record):
        """
        checks the record, first to see if it is the case of column header,
        next to see if the number of fields matches the schema json fields,
        and then datatype and null checks
        """
        is_data = True
        if self.file_path.split('.')[-1] == 'csv':
            if self.header_skip:
                logging.info('Skipping header data... {}'.format(record))
                self.header_skip = False
                is_data = False
                return [(record, None, None, is_data)]
            record_attributes = list(csv.reader([record]))[0]
            if len(record_attributes) != len(self.schema[FIELDS_KEY]):
                if len(record_attributes) > 1 or not record_attributes[0].strip().isdigit():
                    IS_VALID_FILE = False
                is_data = None
                return [(record, None, None, is_data)]
            for record_attribute, attribute_schema in zip(
                record_attributes, self.schema[FIELDS_KEY]):
                is_valid_datatype_check = self.__datatype_check(record_attribute, attribute_schema)
                is_valid_null_check = self.__null_check(record_attribute, attribute_schema)
        return [(record, is_valid_datatype_check, is_valid_null_check, is_data)]


def run(argv=None):
    """Main entry point, defines and runs dataflow pipeline"""
    program_conf = get_program_conf()

    # Test usage of utils... do something with ddl
    print(get_ddl_list('dev')[2])

    # Parse args
    program_args = program_conf[PROGRAM_ARGS_CONF]
    pipeline_args = program_conf[PIPELINE_ARGS_CONF]
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Get schema path key for validation
    gcs_client = storage.Client()
    etl_config_dict = get_gcs_json_as_dict(program_args[ETL_CONFIG_PATH_CONF], gcs_client)
    schema_dict = get_gcs_json_as_dict(program_args[SCHEMA_PATH_KEY], gcs_client)
    input_file_path = program_args[INPUT_CONF]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        validated_records = (
            pipeline 
            | 'read' >> ReadFromText(input_file_path)
            | 'validate' >> beam.ParDo(ValidateRecord(schema_dict, input_file_path, etl_config_dict))
            | 'filter_data' >> beam.Filter(lambda x: x[1] and x[2] and x[3])
            | 'recover_data' >> beam.Map(lambda x: x[0])
            # | beam.Map(print)
        )
        if IS_VALID_FILE:
            validated_records | 'write_success' >> WriteToText(program_args[OUTPUT_CONF])    
        else:
            validated_records | 'write_reject' >> WriteToText(program_args[REJECT_CONF])


if __name__ == '__main__':
    run()
