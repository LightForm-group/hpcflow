"""`hpcflow.project.py`

This module contains a class to represent the root directory of an hpcflow
project.

"""

import shutil
from pathlib import Path
from hpcflow import CONFIG, PROJECTS_DB_DIR, DB_URI
from hpcflow.utils import get_random_hex


class Project(object):

    def __init__(self, dir_path, clean=False):

        self.dir_path = Path(dir_path or '').resolve()
        self.hf_dir = self.dir_path.joinpath(CONFIG['hpcflow_directory'])

        if clean:
            self.clean()

        if not self.hf_dir.exists():

            self.db_directory_name = get_random_hex(10)
            self.hf_dir.mkdir()
            self.project_db_dir.mkdir()

            with self.hf_dir.joinpath('db_dir').open('w') as handle:
                handle.write(self.db_directory_name)

        else:
            with self.hf_dir.joinpath('db_dir').open() as handle:
                self.db_directory_name = handle.read().strip()

        self.db_uri = DB_URI.format(self.project_db_dir.as_posix())

    @property
    def project_db_dir(self):
        return PROJECTS_DB_DIR.joinpath(self.db_directory_name)

    def clean(self):
        if self.hf_dir.exists():
            shutil.rmtree(str(self.hf_dir))
