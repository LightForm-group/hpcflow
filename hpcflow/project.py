"""`hpcflow.project.py`

This module contains a class to represent the root directory of an hpcflow
project.

"""

import os
import shutil
from pathlib import Path
from subprocess import run

from hpcflow.config import Config
from hpcflow.utils import get_random_hex


class Project(object):

    def __init__(self, dir_path, config_dir=None, clean=False):

        Config.set_config(config_dir)
        self.dir_path = Path(dir_path or '').resolve()
        self.hf_dir = self.dir_path.joinpath(Config.get('hpcflow_directory'))

        if clean:
            self.clean()

        if not self.hf_dir.exists():

            self.db_directory_name = get_random_hex(10)
            self.db_path = self.project_db_dir.joinpath(Config.get('DB_name')).as_posix()

            self.hf_dir.mkdir()
            self.project_db_dir.mkdir()

            with self.hf_dir.joinpath('db_path').open('w') as handle:
                handle.write(self.db_path)

        else:

            with self.hf_dir.joinpath('db_path').open() as handle:
                self.db_path = handle.read().strip()
                self.db_directory_name = Path(
                    self.db_path).relative_to(
                        Config.get('projects_DB_dir')).parent

    @property
    def db_uri(self):
        return 'sqlite:///' + self.db_path

    @property
    def project_db_dir(self):
        return Config.get('projects_DB_dir').joinpath(self.db_directory_name)

    @property
    def db_dir_symlink(self):
        return self.hf_dir.joinpath(self.db_directory_name)

    def ensure_db_symlink(self):
        """Add a symlink to the DB for convenience (has to be done after the DB is created)"""

        if not self.db_dir_symlink.exists():
            target = str(self.project_db_dir)
            link = str(self.db_dir_symlink)
            if os.name == 'nt':
                cmd = 'mklink /D "{}" "{}"'.format(link, target)
            elif os.name == 'posix':
                cmd = 'ln --symbolic "{}" "{}"'.format(target, link)

            run(cmd, shell=True)

    def clean(self):
        """Remove all hpcflow related files and directories associated with this project."""

        if self.hf_dir.exists():

            # Remove database:
            shutil.rmtree(str(self.project_db_dir))

            # Remove project directory:
            shutil.rmtree(str(self.hf_dir))
