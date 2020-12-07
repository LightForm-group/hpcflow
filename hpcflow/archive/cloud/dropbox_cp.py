import os
from pathlib import Path
from datetime import datetime

from textwrap import dedent
from typing import Union, List

import dropbox
import dropbox.exceptions
import dropbox.files

from hpcflow.archive.cloud.cloud import CloudProvider
import hpcflow.archive.cloud.cloud as cloud
from hpcflow.archive.cloud.errors import CloudCredentialsError, CloudProviderError
from hpcflow.archive.errors import ArchiveError
from hpcflow.config import Config

# This is the file size limit in bytes that triggers a session upload instead
# of a simple upload.
CHUNK_SIZE = 4 * 1024 * 1024


class DropboxCloudProvider(CloudProvider):
    """A DropboxCloudProvider provides methods for archiving directories and files to Dropbox."""
    def __init__(self, token: str = None):
        if token is None:
            token = self._get_token()
        self.dropbox_connection = dropbox.Dropbox(token)

    @staticmethod
    def _get_token() -> str:
        """Search environment variables and the config for a Dropbox token."""
        env_var_name = 'DROPBOX_TOKEN'
        token = os.getenv(env_var_name)
        if token is None:
            token = Config.get('dropbox_token')
        if token is None:
            msg = ('Please set the Dropbox access token in an environment variable '
                   f'"{env_var_name}", or in the config file as "dropbox_token".')
            raise CloudCredentialsError(msg)
        return token

    def check_access(self) -> bool:
        """Check whether the supplied access token is valid for making a connection to Dropbox."""
        try:
            result = self.dropbox_connection.check_user()
        except dropbox.exceptions.AuthError:
            # This may be triggered by a failure to authenticate
            return False
        except Exception:
            # This can be an error from requests due to some sort of connection problem
            return False
        if result.result == "":
            return True
        else:
            # This is due to some sort of error at Dropbox
            return False

    def get_directories(self, path: Union[str, Path]) -> List[str]:
        """Get a list of sub directories within a dropbox path."""
        path = _normalise_path(path)
        directory_list = []
        for item in self.dropbox_connection.files_list_folder(path).entries:
            if isinstance(item, dropbox.files.FolderMetadata):
                directory_list.append(item.name)
        return directory_list

    def check_directory_exists(self, directory: Union[str, Path]) -> bool:
        """Check a given directory exists on dropbox."""
        directory = _normalise_path(directory)
        try:
            meta = self.dropbox_connection.files_get_metadata(directory)
            return isinstance(meta, dropbox.files.FolderMetadata)
        except dropbox.exceptions.ApiError as err:
            raise CloudProviderError(err)

    def _get_dropbox_file_modified_time(self, dropbox_path: str) -> datetime:
        """Get the last modified time of a file on Dropbox. Raises `dropbox.exceptions.ApiError`
        if the file does not exist."""
        existing_file = self.dropbox_connection.files_get_metadata(dropbox_path)
        if isinstance(existing_file, dropbox.files.FileMetadata):
            return existing_file.client_modified
        else:
            raise ArchiveError("Provided path is not a file.")

    def archive_directory(self, local_path: Union[str, Path], remote_path: Union[str, Path],
                          exclude: List[str] = None):
        """
        Archive a the contents of a local directory into a directory on dropbox.
        Any files in the dropbox directory not in the source directory are ignored.

        Parameters
        ----------
        local_path : Path
            Path of directory on local computer to upload to dropbox.
        remote_path : Path
            Directory on dropbox to upload the file to.
        exclude : list, optional
            List of file or directory names to exclude, matched with `fnmatch` for
            files, or compared directly for directories.

        Notes
        -----
        Does not upload empty directories.

        """
        print('hpcflow.archive.cloud.providers.dropbox.archive_directory', flush=True)

        if exclude is None:
            exclude = []

        local_path = Path(local_path)
        remote_path = Path(remote_path)

        if not local_path.is_dir():
            raise ValueError(f'Specified `local_dir` is not a directory: {local_path}')

        paths_to_upload = generate_files(local_path, exclude)

        for root, files in paths_to_upload.items():
            print(f'Uploading from root directory: {root}', flush=True)
            for file_name in sorted(files):
                src_file = Path(root).joinpath(file_name)
                rel_path = src_file.relative_to(local_path)
                dst_dir = remote_path.joinpath(rel_path.parent)

                print(f'Uploading file: {file_name}', flush=True)
                try:
                    self._archive_file(src_file, dst_dir)
                except ArchiveError:
                    print(f"Could not find local file '{file_name}' at '{root}'. "
                          f"File not archived.")
                    continue
                except CloudProviderError:
                    print(f"Could not upload file '{file_name}' at '{root}', due to a Dropbox"
                          f"error. File Not archived.")
                    continue

    def _archive_file(self, local_path: Path,
                      dropbox_dir: Path) -> Union[dropbox.files.Metadata, None]:
        """Upload a file to a dropbox directory such that if the local file is newer than the
        copy on Dropbox, the newer file overwrites the older file, and if the local file is
        older than the copy on Dropbox, the local file is uploaded with an "auto-incremented"
        name.

        Note that if the modified times are different, the file still won't be uploaded
        by dropbox if the file contents are identical. The point of checking for the
        modified times is to save some time by not having Dropbox check file contents.

        Parameters
        ----------
        local_path : Path
            Path of file on local computer to upload to dropbox.
        dropbox_dir : Path
            Directory on Dropbox into which the file should be uploaded.

        Returns
        --------
        FileMetadata of file if file uploaded. If file not uploaded return None.
        """
        client_modified = _get_client_modified_time(local_path)
        dropbox_path = _normalise_path(Path(dropbox_dir).joinpath(local_path.name))

        try:
            # Try to get the last modified time of the file on Dropbox
            existing_modified = self._get_dropbox_file_modified_time(dropbox_path)
        except dropbox.exceptions.ApiError:
            # File does not exist on dropbox
            return self._upload_file_to_dropbox(local_path, dropbox_dir)
        except Exception:
            raise CloudProviderError('Unexpected error.')

        if client_modified == existing_modified:
            # If file on Dropbox has the same modified time as the client file then don't upload.
            return None
        elif client_modified < existing_modified:
            # If the client file is older than the file on dropbox
            return self._upload_file_to_dropbox(local_path, dropbox_dir, False, client_modified)
        else:
            # If the client file is newer than the one on dropbox then overwrite the one on dropbox
            return self._upload_file_to_dropbox(local_path, dropbox_dir, True, client_modified)

    def _upload_file_to_dropbox(self, local_path: Union[str, Path], dropbox_dir: Union[str, Path],
                                overwrite=False,
                                client_modified=None) -> dropbox.files.FileMetadata:
        """
        Parameters
        ----------
        local_path : str or Path
            Path of file on local computer to upload to dropbox.
        dropbox_dir : str or Path
            Directory on Dropbox into which the file should be uploaded.
        overwrite : bool
            If True, the file overwrites an existing file with the same name.
        client_modified: datetime
            Dropbox uses this time as the "last modified" file metadata. If None it defaults
            to the upload time.
        """
        local_path = Path(local_path)
        dropbox_path = Path(dropbox_dir).joinpath(local_path.name)
        dropbox_path = _normalise_path(dropbox_path)

        overwrite_mode = _get_overwrite_mode(overwrite)

        try:
            file_metadata = self._select_upload_method(local_path, dropbox_path, overwrite_mode,
                                                       client_modified)
        except Exception as err:
            raise CloudProviderError(f'Cloud provider error: {err}')
        else:
            return file_metadata

    def _select_upload_method(self, local_path: Path, dropbox_path: str,
                              overwrite_mode: dropbox.dropbox.files.WriteMode,
                              client_modified: Union[datetime, None]):
        """This function determines the size of the file to be uploaded and then
        selects an upload method based on the file size."""
        file_size = os.path.getsize(local_path)
        if file_size <= CHUNK_SIZE:
            return self.simple_upload(local_path, dropbox_path, overwrite_mode, client_modified)
        else:
            return self.session_upload(dropbox_path, file_size, local_path, overwrite_mode,
                                       client_modified)

    def session_upload(self, dropbox_path: str, file_size: int,
                       local_path: Path, overwrite_mode,
                       client_modified) -> dropbox.files.FileMetadata:
        """Upload a file to Dropbox using an Upload session. This is required for files > 150 MB.
        Though is probably beneficial for smaller files as it allows progress to be monitored as
        the upload progresses."""
        with open(local_path, 'rb') as f:
            upload_session = self.dropbox_connection.files_upload_session_start(f.read(CHUNK_SIZE))
            cursor = dropbox.files.UploadSessionCursor(session_id=upload_session.session_id,
                                                       offset=f.tell())
            commit = dropbox.files.CommitInfo(path=dropbox_path, mode=overwrite_mode,
                                              client_modified=client_modified)
            while f.tell() < file_size:
                if (file_size - f.tell()) > CHUNK_SIZE:
                    self.dropbox_connection.files_upload_session_append_v2(f.read(CHUNK_SIZE),
                                                                           cursor)
                    cursor.offset = f.tell()
                else:
                    file_metadata = self.dropbox_connection.files_upload_session_finish(
                        f.read(CHUNK_SIZE), cursor, commit)
        return file_metadata

    def simple_upload(self, local_path: Path, dropbox_path: str,
                      overwrite: dropbox.dropbox.files.WriteMode,
                      client_modified: Union[datetime, None]) -> dropbox.files.FileMetadata:
        """Upload a file using a simple non-session upload. This is good for smaller files."""
        file_contents = cloud.read_file_contents(local_path)
        return self.dropbox_connection.files_upload(file_contents, dropbox_path,
                                                    overwrite, True,
                                                    client_modified)


def generate_files(local_path: Path, exclude: List[str] = None) -> dict:
    """Generate a dictionary of files to be uploaded by recursing through a directory using
    `os.walk`."""
    file_list = {}
    for root, dirs, files in os.walk(str(local_path)):
        if exclude:
            files = cloud.exclude_specified_files(files, exclude)
            # Modifying dirs in place changes the behavior of os.walk, preventing it
            # from recursing into removed directories on later iterations.
            dirs[:] = cloud.exclude_specified_directories(dirs, exclude)
        file_list[root] = sorted(files)
    return file_list


def _get_overwrite_mode(overwrite: bool) -> dropbox.dropbox.files.WriteMode:
    """Get the tag that determines behaviour when an uploaded file already exists
    in the destination. If `overwrite` is True then overwrite existing files else add the new
    file with a different name, leaving the old file."""
    if overwrite:
        return dropbox.dropbox.files.WriteMode('overwrite')
    else:
        return dropbox.dropbox.files.WriteMode('add')


def _get_client_modified_time(local_path: Path):
    """Gets the last modified time of a local file and converts it to a dropbox compatible time by
    culling the microseconds."""
    ts_sec = local_path.stat().st_mtime
    dt = datetime.utcfromtimestamp(ts_sec).replace(microsecond=0)
    return dt


def _normalise_path(path: Union[str, Path]) -> str:
    """Modify a path (str or Path) such that it is a Dropbox-compatible path string."""
    # The path of the Dropbox root directory is an empty string.
    path = str(path)
    if path == ".":
        return ""
    # All dropbox paths must be posix style and prepended by a forward slash.
    path = path.replace('\\', '/')
    if not path.startswith('/'):
        path = '/' + path
    return path


def get_auth_code() -> str:
    """In order to connect to dropbox HPCFlow must be authenticated with Dropbox as a valid app.
    This is done by the user getting an authorisation token from a url and providing it to hpcflow.
    hpcflow then uses this auth token to get an API token from Dropbox.

    This function prompts the user to go to the auth token url, accepts it as input and gets and
    returns the subsequent API token.
    """
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
    return oauth_result.access_token
