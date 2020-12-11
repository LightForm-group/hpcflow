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


class FileToUpload:
    """An object containing information about a file to be uploaded."""
    def __init__(self, local_path: Path, remote_directory: Path):
        """
        Parameters
        ----------
        local_path
            The path of the file to upload
        remote_directory
            The directory on Dropbox into which to upload the file
        """
        # The full local path of the file.
        self.path = local_path

        # The path on Dropbox the file will have including name
        self.dropbox_path = _normalise_path(remote_directory / local_path.name)

        self.last_modified = self._get_last_modified_time()
        self.size = os.path.getsize(self.path)

        # Default behavior is to not overwrite files in the destination
        self.overwrite_mode = dropbox.dropbox.files.WriteMode('add')

    def __eq__(self, other):
        if self.path == other.path:
            return True
        else:
            return False

    def _get_last_modified_time(self):
        """Gets the last modified time of the file to be uploaded and convert it to a dropbox
        compatible time by culling the microseconds.

        Notes
        -------
        Dropbox culls microseconds from the datetime passed as the
         `client_modified` parameter (it does not round).
         Ref: (https://www.dropboxforum.com/t5/API-Support-Feedback/Python-API-client
         -modified-resolution/m-p/362170/highlight/true#M20596)
        """
        ts_sec = self.path.stat().st_mtime
        dt = datetime.utcfromtimestamp(ts_sec).replace(microsecond=0)
        return dt


class DropboxCloudProvider(CloudProvider):
    """A DropboxCloudProvider provides methods for archiving directories and their
    contents to Dropbox."""
    def __init__(self, token: str = None):
        if token is None:
            token = self._get_token()
        self.dropbox_connection = dropbox.Dropbox(token)

    @staticmethod
    def _get_token() -> str:
        """Try to find a Dropbox token. First search environment variables and
        then the hpcflow config. If no token found then raise an Exception."""
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
        """Get a list of sub directories within a dropbox path.

        Parameters
        ----------
        path
            The dropbox path to list the directories of. Path is relative to the Dropbox root.
        """
        path = _normalise_path(path)
        directory_list = []
        for item in self.dropbox_connection.files_list_folder(path).entries:
            if isinstance(item, dropbox.files.FolderMetadata):
                directory_list.append(item.name)
        return directory_list

    def check_directory_exists(self, directory: Union[str, Path]) -> bool:
        """Check a given directory exists on dropbox.

        Parameters
        ----------
        directory
            The directory on Dropbox to check. Path is relative to the Dropbox root.
        """
        directory = _normalise_path(directory)
        try:
            meta = self.dropbox_connection.files_get_metadata(directory)
            return isinstance(meta, dropbox.files.FolderMetadata)
        except dropbox.exceptions.ApiError as err:
            raise CloudProviderError(err)

    def _get_dropbox_file_modified_time(self, dropbox_path: str) -> Union[datetime, None]:
        """Get the last modified time of a file on Dropbox. Returns None if the file
        does not exist.

        Parameters
        ----------
        dropbox_path
            The path of a file on Dropbox to check. Path is relative to the Dropbox root.
        """
        try:
            existing_file = self.dropbox_connection.files_get_metadata(dropbox_path)
        except dropbox.exceptions.ApiError:
            return None
        if isinstance(existing_file, dropbox.files.FileMetadata):
            return existing_file.client_modified
        else:
            raise ArchiveError("Provided path is not a file.")

    def archive_directory(self, dir_to_upload: Union[str, Path], remote_dir: Union[str, Path],
                          exclude: List[str] = None):
        """
        Archive a the contents of a local directory into a directory on dropbox.
        Any files in the dropbox directory not in the source directory are ignored.

        Parameters
        ----------
        dir_to_upload
            Path of directory on local computer to upload to dropbox.
        remote_dir
            Directory on dropbox to upload the file to.
        exclude
            List of file or directory names to exclude, matched with `fnmatch` for
            files, or compared directly for directories.

        Notes
        -----
        Does not upload empty directories.

        """
        print('hpcflow.archive.cloud.providers.dropbox.archive_directory', flush=True)

        if exclude is None:
            exclude = []

        dir_to_upload = Path(dir_to_upload)
        remote_dir = Path(remote_dir)

        if not dir_to_upload.is_dir():
            raise ValueError(f'Specified `local_dir` is not a directory: {dir_to_upload}')

        files_to_upload = generate_files(dir_to_upload, remote_dir, exclude)

        for file in files_to_upload:
            print(f"Uploading '{file.path.name}' from root '{file.path.root}'", flush=True)
            try:
                self._archive_file(file)
            except ArchiveError:
                print(f"Could not find local file '{file.path.name}' at '{file.path.root}'. "
                      f"File not archived.")
                continue
            except CloudProviderError:
                print(f"Could not upload file '{file.path.name}' at '{file.path.root}',"
                      f" due to a Dropbox error. File Not archived.")
                continue

    def _archive_file(self, file: FileToUpload) -> Union[dropbox.files.Metadata, None]:
        """Upload a file to a dropbox directory such that if the local file is newer than the
        copy on Dropbox, the newer file overwrites the older file, and if the local file is
        older than the copy on Dropbox, the local file is uploaded with an "auto-incremented"
        name.

        Note that if the modified times are different, the file still won't be uploaded
        by dropbox if the file contents are identical. The point of checking for the
        modified times is to save some time by not having Dropbox check file contents.

        Parameters
        ----------
        file
            `FileToUpload` object describing file to be uploaded

        Returns
        --------
        FileMetadata of file if file uploaded. If file not uploaded return None.
        """
        # Get the last modified time of the file on Dropbox
        dropbox_modified = self._get_dropbox_file_modified_time(file.dropbox_path)
        if not dropbox_modified:
            # File does not exist on dropbox
            return self._choose_session_type(file)
        if file.last_modified == dropbox_modified:
            # If file on Dropbox has the same modified time as the client file then don't upload.
            return None
        elif file.last_modified > dropbox_modified:
            # If the client file is newer than the one on dropbox then overwrite the one on dropbox
            file.overwrite_mode = dropbox.dropbox.files.WriteMode('overwrite')

        return self._choose_session_type(file)

    def _choose_session_type(self, file: FileToUpload) -> dropbox.files.FileMetadata:
        """Choose whether to upload the file as a simple upload or a session upload
        depending on its size.

        Parameters
        ----------
        file
            `FileToUpload` object describing file to be uploaded
        """

        try:
            if file.size <= CHUNK_SIZE:
                return self.simple_upload(file)
            else:
                return self.session_upload(file)
        except Exception as err:
            raise CloudProviderError(f'Cloud provider error: {err}')

    def session_upload(self, file: FileToUpload) -> dropbox.files.FileMetadata:
        """Upload a file to Dropbox using an Upload session. This is required for files > 150 MB.
        Though is probably beneficial for smaller files as it allows progress to be monitored as
        the upload progresses.

        Parameters
        ----------
        file
            `FileToUpload` object describing file to be uploaded
        """
        with open(file.path, 'rb') as f:
            upload_session = self.dropbox_connection.files_upload_session_start(f.read(CHUNK_SIZE))
            cursor = dropbox.files.UploadSessionCursor(session_id=upload_session.session_id,
                                                       offset=f.tell())
            commit = dropbox.files.CommitInfo(path=file.dropbox_path, mode=file.overwrite_mode,
                                              client_modified=file.last_modified)
            while f.tell() < file.size:
                if (file.size - f.tell()) > CHUNK_SIZE:
                    self.dropbox_connection.files_upload_session_append_v2(f.read(CHUNK_SIZE),
                                                                           cursor)
                    cursor.offset = f.tell()
                else:
                    file_metadata = self.dropbox_connection.files_upload_session_finish(
                        f.read(CHUNK_SIZE), cursor, commit)
        return file_metadata

    def simple_upload(self, file: FileToUpload) -> dropbox.files.FileMetadata:
        """Upload a file using a simple non-session upload. This is good for smaller files.

        Parameters
        ----------
        file
            `FileToUpload` object describing file to be uploaded
        """
        file_contents = cloud.read_file_contents(file.path)
        return self.dropbox_connection.files_upload(file_contents, file.dropbox_path,
                                                    file.overwrite_mode, True, file.last_modified)


def generate_files(dir_to_upload: Path, remote_directory: Path,
                   exclude: List[str] = None) -> List[FileToUpload]:
    """Generate a list of FilesToUpload objects describing the files to be uploaded
    by recursing through a directory using `os.walk`.

    Parameters
    ----------
        dir_to_upload
            The path of the local directory to upload.
        remote_directory
            Where on Dropbox the files will be uploaded to. Path is relative to the Dropbox root.
        exclude
            List of files and folder names to exclude. Matched with `fnmatch` for
            files, or compared directly for directories.
    """
    file_list = []
    for file_root, dirs, files in os.walk(str(dir_to_upload)):
        if exclude:
            files = cloud.exclude_specified_files(files, exclude)
            # Modifying dirs in place changes the behavior of os.walk, preventing it
            # from recursing into removed directories on later iterations.
            dirs[:] = cloud.exclude_specified_directories(dirs, exclude)
        for file_name in files:
            # The path of the file to upload
            local_path = Path(file_root) / Path(file_name)
            # The path of the file relative to the upload directory
            local_rel_path = local_path.relative_to(dir_to_upload)
            # The folder on dropbox the file will be put into
            dropbox_path = remote_directory / local_rel_path
            file_list.append(FileToUpload(local_path, dropbox_path))
    return file_list


def _normalise_path(path: Union[str, Path]) -> str:
    """Modify a path (str or Path) such that it is a Dropbox-compatible path string."""
    path = str(path)
    # The path of the Dropbox root directory is a forward slash.
    if path == ".":
        return "/"
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
