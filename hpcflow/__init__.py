"""`hpcflow.__init__.py`"""

import os
import yaml
from pathlib import Path
import shutil

from hpcflow._version import __version__

PKG_DATA_DIR = Path(__file__).parent.joinpath('data')

DATA_DIR = Path(os.getenv('HPCFLOW_DATA_DIR', '~/.hpcflow')).expanduser()
DATA_DIR.mkdir(exist_ok=True)
PROFILES_DIR = DATA_DIR.joinpath('profiles')

_CONFIG_PATH = DATA_DIR.joinpath('_config.yml')
if not _CONFIG_PATH.is_file():
    # If no config file in data directory, copy the default config file:
    shutil.copyfile(
        str(PKG_DATA_DIR.joinpath('_config_default.yml')),
        str(_CONFIG_PATH)
    )

with _CONFIG_PATH.open('r') as handle:
    CONFIG = yaml.safe_load(handle)
    # TODO: checks on config vals; e.g. format of `submission_filename_fmt`

_VARS_LOOKUP_PATH = DATA_DIR.joinpath('_variable_lookup.yml')
with _VARS_LOOKUP_PATH.open('r') as handle:
    VARS_LOOKUP = yaml.safe_load(handle)
