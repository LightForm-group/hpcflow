import os
from pathlib import Path
from warnings import warn

from ruamel.yaml import YAML

from hpcflow.errors import ConfigurationError


class Config(object):

    __PROFILE_KEYS_REQ = [
        'command_groups',
        'profile_name',
        'profile_order',
    ]
    __PROFILE_KEYS_GOOD = __PROFILE_KEYS_REQ + [
        'alternate_scratch',
        'archive',
        'archive_excludes',
        'directory',
        'inherits',
        'is_job_array',
        'loop',
        'environment',
        'nesting',
        'scheduler',
        'scheduler_options',
        'output_dir',
        'error_dir',
        'pre_commands',
        'root_archive',
        'root_archive_excludes',
        'variable_scope',
        'variables',
        'stats',
    ]
    __CMD_GROUP_KEYS_REQ = [
        'commands',
    ]
    __CMD_GROUP_KEYS_GOOD = __CMD_GROUP_KEYS_REQ + [
        'alternate_scratch',
        'archive',
        'archive_excludes',
        'directory',
        'is_job_array',
        'environment',
        'nesting',
        'scheduler',
        'scheduler_options',
        'output_dir',
        'error_dir',
        'profile_name',
        'profile_order',
        'exec_order',
        'stats',
        'job_name',
    ]
    __CMD_GROUP_DEFAULTS = {
        'is_job_array': True,
        'nesting': None,
        'directory': '',
        'archive': None,
        'archive_excludes': [],
        'scheduler': 'direct',
        'scheduler_options': {},
        'output_dir': None,  # Set in `Config.set_config`
        'error_dir': None,  # Set in `Config.set_config`
        'stats': False,
    }

    __CONSTANTS = {
        'DB_name': 'workflow.db',
        'alt_scratch_exc_file': 'alt_scratch_exclude',
        'alt_scratch_exc_file_ext': '.txt',
        'profile_keys_required': __PROFILE_KEYS_REQ,
        'profile_keys_allowed': __PROFILE_KEYS_GOOD,
        'cmd_group_keys_required': __CMD_GROUP_KEYS_REQ,
        'cmd_group_keys_allowed': __CMD_GROUP_KEYS_GOOD,
        'cmd_group_defaults': __CMD_GROUP_DEFAULTS,
    }

    # These may be customised in the config file:
    __ALLOWED = {
        'data_dir': None,
        'variable_delimiters': ['<<', '>>'],
        'default_cmd_group_dir_var_name': '__hpcflow_cmd_group_directory_var',
        'profile_filename_fmt': '<<profile_order>>.<<profile_name>>.yml',
        'profile_ext': '.yml',
        'jobscript_ext': '.sh',
        'variable_file_ext': '.txt',
        'working_dirs_file_ext': '.txt',
        'default_output_dir': 'output',
        'default_error_dir': 'output',
        'hpcflow_directory': '.hpcflow',
        'archives': [],
    }

    __conf = {}

    is_set = False

    @staticmethod
    def set_config(config_dir=None):
        'Load configuration from a YAML file.'

        # TODO: checks on config vals; e.g. format of `profile_filename_fmt`?

        if not config_dir:
            config_dir = Path(os.getenv('HPCFLOW_CONFIG_DIR', '~/.hpcflow')).expanduser()
        else:
            config_dir = Path(config_dir)

        if Config.is_set:
            if config_dir != Config.get('config_dir'):
                warn(f'Config is already set, but `config_dir` changed from '
                     f'"{Config.get("config_dir")}" to "{config_dir}".')
            return

        if not config_dir.is_dir():
            print('Configuration directory does not exist. Generating.')
            config_dir.mkdir()

        config_file = config_dir.joinpath('config.yml')
        if not config_file.is_file():
            print('No config.yml found. Generating an empty config.yml file.')
            with config_file.open('w'):
                pass

        print(f'Loading config from {config_file}')

        yaml = YAML()
        config_dat = yaml.load(config_file) or {}
        bad_keys = list(set(config_dat.keys()) - set(Config.__ALLOWED.keys()))
        if bad_keys:
            bad_keys_fmt = ', '.join([f'"{i}"' for i in bad_keys])
            raise ConfigurationError(f'Unknown configuration options: {bad_keys_fmt}.')

        profiles_dir = config_dir.joinpath('profiles')
        if not profiles_dir.is_dir():
            print('Profiles directory does not exist. Generating.')
            profiles_dir.mkdir()

        projects_DB_dir = config_dir.joinpath('projects')
        if not projects_DB_dir.is_dir():
            print('Projects database directory does not exist. Generating.')
            projects_DB_dir.mkdir()

        var_look_file = config_dir.joinpath('variable_lookup.yml')
        if not var_look_file.is_file():
            print('No variable lookup file found. Generating.')
            var_look_dat = {'variable_templates': {}, 'scopes': {}}
            yaml.dump(var_look_dat, var_look_file)
        else:
            var_look_dat = yaml.load(var_look_file)

        if sorted(var_look_dat.keys()) != ['scopes', 'variable_templates']:
            msg = (f'Variable lookup file must have keys "scopes" (dict) and '
                   f'"variable_templates" (dict): {var_look_file}.')
            raise ConfigurationError(msg)

        Config.__conf.update(Config.__ALLOWED)
        Config.__conf.update(config_dat)
        Config.__conf.update({
            'config_dir': config_dir,
            'profiles_dir': profiles_dir,
            'projects_DB_dir': projects_DB_dir,
            'variable_lookup': var_look_dat,
        })
        Config.__conf.update(Config.__CONSTANTS)
        Config.__conf['cmd_group_defaults']['output_dir'] = (
            Config.__conf['default_output_dir']
        )
        Config.__conf['cmd_group_defaults']['error_dir'] = (
            Config.__conf['default_error_dir']
        )

        Config.is_set = True

    @staticmethod
    def get(name):
        if not Config.is_set:
            raise ConfigurationError('Configuration is not yet set.')
        return Config.__conf[name]
