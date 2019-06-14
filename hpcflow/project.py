"""`hpcflow.project.py`

This module contains a class to represent the root directory of an hpcflow
project.

"""

import shutil
from pathlib import Path
from hpcflow import CONFIG


class Project(object):

    DB_URI = 'sqlite:///{}/workflows.db'

    def __init__(self, dir_path, clean=False):

        self.dir_path = Path(dir_path or '').resolve()
        self.hf_dir = self.dir_path.joinpath(CONFIG['hpcflow_directory'])
        self.db_uri = Project.DB_URI.format(self.hf_dir.as_posix())

        if clean:
            self.clean()

        if not self.hf_dir.exists():
            self.hf_dir.mkdir()

    def clean(self):
        if self.hf_dir.exists():
            shutil.rmtree(str(self.hf_dir))
