"""`hpcflow.archive.cloud.cloud.py"""

import enum

from hpcflow.archive.cloud.providers import dropbox


class CloudProvider(enum.Enum):

    dropbox = 'dropbox'
    onedrive = 'onedrive'
    null = ''

    def check_access(self):

        print('hpcflow.archive.cloud.CloudProvider.check_access', flush=True)

        if self.name == 'dropbox':
            dropbox.check_access()

    def upload(self, local_path, remote_path, exclude):

        print('hpcflow.archive.cloud.CloudProvider.upload', flush=True)

        if self.name == 'dropbox':
            dbx = dropbox.get_dropbox()
            dropbox.upload_dropbox_dir(
                dbx,
                local_path,
                remote_path,
                overwrite=True,
                exclude=exclude,
            )
