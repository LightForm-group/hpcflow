"""`hpcflow.archive.cloud.cloud.py

TODO: if we have multiple cloud providers, perhaps it would be better to
implement the `upload_directory` function generally, and then just rely on 
the provider-specific `upload_file` functions.

"""

import enum

from hpcflow.archive.cloud.providers import dropbox


class CloudProvider(enum.Enum):

    dropbox = 'dropbox'
    onedrive = 'onedrive'
    null = ''

    def check_access(self):
        if self.name == 'dropbox':
            dropbox.check_access()

    def archive_directory(self, local_path, remote_path, exclude):
        if self.name == 'dropbox':
            dropbox.archive_directory(
                dropbox.get_dropbox(), local_path, remote_path, exclude)

    def get_directories(self, path):
        """Get sub directories within a path"""

        if self.name == 'dropbox':
            dbx = dropbox.get_dropbox()
            return dropbox.get_folders(dbx, path)

    def check_exists(self, directory):
        """Check a given directory exists on the cloud storage."""
        if self.name == 'dropbox':
            directory = dropbox.normalise_path(directory)
            dbx = dropbox.get_dropbox()
            return dropbox.is_folder(dbx, directory)

    def get_token(self):
        if self.name == 'dropbox':
            return dropbox.get_token()
