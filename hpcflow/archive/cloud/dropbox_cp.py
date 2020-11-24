import os
from pathlib import Path
import posixpath
import fnmatch
from datetime import datetime

from textwrap import dedent
from typing import Union, List

import dropbox
import dropbox.exceptions
import dropbox.files

from hpcflow.archive.cloud.cloud import CloudProvider
from hpcflow.archive.cloud.errors import CloudCredentialsError, CloudProviderError
from hpcflow.archive.errors import ArchiveError
from hpcflow.config import Config


class DropboxCloudProvider(CloudProvider):
    def __init__(self, token: str = None):
        env_var_name = 'DROPBOX_TOKEN'
        if token is None:
            token = os.getenv(env_var_name)
        if token is None:
            token = Config.get('dropbox_token')
        if token is None:
            msg = ('Please set the Dropbox access token in an environment variable '
                   f'"{env_var_name}", or in the config file as "dropbox_token".')
            raise CloudCredentialsError(msg)

        self.dropbox_connection = dropbox.Dropbox(token)

    def check_access(self) -> bool:
        """Check whether the supplied access token is valid for making a connection to Dropbox."""
        try:
            result = self.dropbox_connection.check_user()
        except dropbox.exceptions.AuthError:
            return False
        if result.result == "":
            return True
        else:
            return False

    def archive_directory(self, local_path, remote_path, exclude):
        """Archive a the contents of a local directory into a directory on dropbox.
        Any files in the dropbox directory not in the source directory are ignored.

        Parameters
        ----------
        local_path : str or Path
            Path of directory on local computer to upload to dropbox.
        remote_path : str or Path
            Directory on dropbox to upload the files to.
        exclude : list, optional
            List of file or directory names to exclude, matched with `fnmatch` for
            files, or compared directly for directories.
        """

        print('hpcflow.archive.cloud.providers.dropbox.archive_directory', flush=True)

        local_dir = Path(local_path)
        dropbox_dir = Path(remote_path)
        self._upload_dropbox_dir(local_dir, dropbox_dir, exclude=exclude, archive=True)

    def _upload_dropbox_dir(self, local_dir, dropbox_dir, overwrite=False, auto_rename=False,
                            exclude=None, archive=False):
        """
        Parameters
        ----------
        local_dir : Path
            Path of directory on local computer to upload to dropbox.
        dropbox_dir : Path
            Directory on dropbox to upload the file to.
        overwrite : bool
            If True, the file overwrites an existing file with the same name.
        auto_rename : bool
            If True, rename the file if there is a conflict.
        exclude : list, optional
            List of file or directory names to exclude, matched with `fnmatch` for
            files, or compared directly for directories.
        archive : bool, optional

        Notes
        -----
        Does not upload empty directories.

        """
        # Validation
        if exclude is None:
            exclude = []

        if not local_dir.is_dir():
            raise ValueError('Specified `local_dir` is not a directory: {}'.format(local_dir))

        for root, dirs, files in os.walk(str(local_dir)):

            root_test = Path(root)
            dirs[:] = [d for d in dirs if d not in exclude]
            print('Uploading from root directory: {}'.format(root), flush=True)

            for file_name in sorted(files):
                up_file = False
                if exclude is not None:
                    if not any([fnmatch.fnmatch(file_name, i) for i in exclude]):
                        up_file = True
                else:
                    up_file = True

                if up_file:
                    src_file = root_test.joinpath(file_name)
                    rel_path = src_file.relative_to(local_dir)
                    dst_dir = dropbox_dir.joinpath(rel_path.parent)

                    print('Uploading file: {}'.format(file_name), flush=True)
                    try:
                        if archive:
                            self._archive_file(src_file, dst_dir)
                        else:
                            self._upload_file_to_dropbox(src_file, dst_dir, overwrite, auto_rename)
                    except ArchiveError as err:
                        print(f'Archive error: {err}', flush=True)
                        continue

                    except CloudProviderError as err:
                        print(f'Cloud provider error: {err}', flush=True)
                        continue

    def _archive_file(self, local_path: Union[str, Path], dropbox_dir: Union[str, Path],
                      check_modified_time=True):
        """Upload a file to a dropbox directory such that if the local file is newer than the
        copy on Dropbox, the newer file overwrites the older file, and if the local file is
        older than the copy on Dropbox, the local file is uploaded with an "auto-incremented"
        name.

        Note that if the modified times are different, the file still won't be uploaded
        by dropbox if the file contents are identical. The point of checking for the
        modified times is to save some time by not having Dropbox check file contents.

        Parameters
        ----------
        local_path : str or Path
            Path of file on local computer to upload to dropbox.
        dropbox_dir : str or Path
            Directory on Dropbox into which the file should be uploaded.

        """
        overwrite = False
        auto_rename = False
        client_modified = None

        local_path = Path(local_path)
        dropbox_path = self._normalise_path(Path(dropbox_dir).joinpath(local_path.name))

        if not check_modified_time:
            overwrite = True

        else:
            # Note: Dropbox culls microseconds from the datetime passed as the
            # `client_modified` parameter (it does not round).
            # Ref: ("https://www.dropboxforum.com/t5/API-Support-Feedback/Python-API-client"
            #           "-modified-resolution/m-p/362170/highlight/true#M20596")
            ts_sec = local_path.stat().st_mtime_ns / 1e9
            dt = datetime.utcfromtimestamp(ts_sec)
            client_modified = datetime(  # no microseconds
                year=dt.year,
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                second=dt.second,
            )

            try:
                # Check for existing file
                existing_file = self.dropbox_connection.files_get_metadata(dropbox_path)
                existing_modified = existing_file.client_modified

                if client_modified == existing_modified:
                    print('client_modified time same!')
                    return

                elif client_modified < existing_modified:
                    auto_rename = True
                    print('client file older!')

                elif client_modified > existing_modified:
                    overwrite = True
                    print('client file newer!')

            except dropbox.exceptions.ApiError:
                print('File does not exist.')

            except Exception:
                msg = 'Unexpected error.'
                raise CloudProviderError(msg)

        self._upload_file_to_dropbox(local_path, dropbox_path, overwrite, auto_rename,
                                     client_modified)

    def _upload_file_to_dropbox(self, local_path: Union[str, Path],
                                dropbox_dir: Union[str, Path],
                                overwrite=False, auto_rename=True, client_modified=None):
        """
        Parameters
        ----------
        local_path : str or Path
            Path of file on local computer to upload to dropbox.
        dropbox_dir : str or Path
            Directory on Dropbox into which the file should be uploaded.
        overwrite : bool
            If True, the file overwrites an existing file with the same name.
        auto_rename : bool
            If True, rename the file if there is a conflict.
        client_modified: datetime
            Dropbox uses this time as the "last modified" file metadata. If None it defaults
            to the upload time.
        """
        local_path = Path(local_path)
        dropbox_path = Path(dropbox_dir).joinpath(local_path.name)
        dropbox_path = self._normalise_path(dropbox_path)

        mode = self._get_overwrite_mode(overwrite)

        try:
            handle = local_path.open(mode='rb')
        except FileNotFoundError as err:
            raise ArchiveError(err)

        try:
            self.dropbox_connection.files_upload(handle.read(), dropbox_path, mode,
                                                 auto_rename, client_modified)
        except dropbox.exceptions.ApiError as err:
            raise CloudProviderError(f'Cloud provider error. {err}')
        except Exception:
            raise CloudProviderError('Unexpected error.')

        handle.close()

    @staticmethod
    def _get_overwrite_mode(overwrite: bool) -> dropbox.dropbox.files.WriteMode:
        """Get the tag that determines behaviour when an uploaded file already exists
        in the destination. If `overwrite` is True then overwrite existing files else add the new
        file with a different name, leaving the old file."""
        if overwrite:
            return dropbox.dropbox.files.WriteMode('overwrite')
        else:
            return dropbox.dropbox.files.WriteMode('add')

    def get_directories(self, path: Union[str, Path]) -> List[str]:
        """Get a list of sub directories within a path"""
        path = self._normalise_path(path)
        directory_list = []
        for item in self.dropbox_connection.files_list_folder(path).entries:
            if isinstance(item, dropbox.files.FolderMetadata):
                directory_list.append(item.name)
        return directory_list

    def check_directory_exists(self, directory: Union[str, Path]):
        """Check a given directory exists on the cloud storage."""
        directory = self._normalise_path(directory)
        try:
            meta = self.dropbox_connection.files_get_metadata(directory)
            return isinstance(meta, dropbox.files.FolderMetadata)
        except dropbox.exceptions.ApiError:
            return False

    @staticmethod
    def _normalise_path(path) -> str:
        """Modify a path (str or Path) such that it is a Dropbox-compatible path string."""
        # The path of the Dropbox root directory is an empty string.
        if path == ".":
            return ""
        # It is not clear what this line is doing.
        path = posixpath.join(*str(path).split(os.path.sep))
        # All dropbox paths must be prepended by a forward slash.
        if not path.startswith('/'):
            path = '/' + path
        return path


def get_token() -> str:
    APP_KEY = Config.get('dropbox_app_key')
    auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(APP_KEY, use_pkce=True)
    authorize_url = auth_flow.start()

    msg = dedent(f"""
    --------------------------- Connecting hpcflow to Dropbox ----------------------------

        1. Go to this URL:

        {authorize_url}

        2. Click "Allow" (you might have to log in first).
        3. Copy the authorization code below.

    --------------------------------------------------------------------------------------
    """)
    print(msg)

    auth_code = input('Enter the authorization code here: ').strip()
    oauth_result = auth_flow.finish(auth_code)
    token = oauth_result.access_token

    return token
