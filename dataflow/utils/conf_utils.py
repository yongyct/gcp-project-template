import __main__
import yaml
import argparse
import inspect
from pathlib import Path

from .constants import VALID_ENVS, PIPELINE_ARGS_CONF


def get_program_conf(env=None):
    """Parse and retrieve program configurations"""
    if env is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('-e', '--environment', 
            dest='environment', type=str, required=True,
            help='environment where code is run'
        )
        program_args = parser.parse_args()
        env = program_args.environment.lower()
        file_prefix = Path(__main__.__file__).stem
    else:
        file_prefix = 'utils/' + Path(inspect.getmodule(inspect.stack()[1][0]).__file__).stem
    
    # Validations
    if env not in VALID_ENVS:
        raise InvalidConfigError('Invalid environment provided ({}), permissible values: {}'
                                .format(env, VALID_ENVS))
    with open('./etc/{}_{}.yml'.format(file_prefix, env.lower()), 'r') as conf_file:
        program_conf = yaml.safe_load(conf_file)

    # Reformatting to fit apache beam's format for pipeline args
    if PIPELINE_ARGS_CONF in program_conf:
        pipeline_args_val = []
        for k, v in program_conf[PIPELINE_ARGS_CONF].items():
            pipeline_args_val.append('--' + k)
            pipeline_args_val.append(v)
        program_conf[PIPELINE_ARGS_CONF] = pipeline_args_val

    return program_conf
