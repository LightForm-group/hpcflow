"""`hpcflow.archive.cloud.providers.dropbox.py`

This module provides interactivity with a Dropbox account for archiving.

"""

import os
import posixpath
import fnmatch
from pathlib import Path
from datetime import datetime
from textwrap import dedent

import dropbox as dropbox_api

from hpcflow.config import Config
from hpcflow.archive.errors import ArchiveError
from hpcflow.archive.cloud.errors import CloudProviderError, CloudCredentialsError


def get_token():

    APP_KEY = Config.get('dropbox_app_key')
    auth_flow = dropbox_api.DropboxOAuth2FlowNoRedirect(APP_KEY, use_pkce=True)
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


def get_dropbox():
    env_var_name = 'DROPBOX_TOKEN'
    token = Config.get('dropbox_token') or os.getenv(env_var_name)
    if not token:
        msg = ('Please set the Dropbox access token in an environment variable '
               f'"{env_var_name}", or in the config file as "dropbox_token".')
        raise CloudCredentialsError(msg)

    dbx = dropbox_api.Dropbox(token)

    return dbx


def check_access():
    dbx = get_dropbox()
    try:
        user_acc = dbx.users_get_current_account()
    except:
        msg = 'Could not connect to Dropbox using the supplied access token.'
        raise ValueError(msg)

    print('OK!', flush=True)


def get_files(dbx, path):
    path = normalise_path(path)
    out = []
    for i in dbx.files_list_folder(path).entries:
        if isinstance(i, dropbox_api.files.FileMetadata):
            out.append(i.name)
    return out


def get_folders(dbx, path):
    path = normalise_path(path)
    out = []
    for i in dbx.files_list_folder(path).entries:
        if isinstance(i, dropbox_api.files.FolderMetadata):
            out.append(i.name)
    return out


def is_file(dbx, path):
    """Check given path on dropbox is a file."""
    meta = dbx.files_get_metadata(path)
    return isinstance(meta, dropbox_api.files.FileMetadata)


def is_folder(dbx, path):
    """Check given path on dropbox is a folder."""
    try:
        meta = dbx.files_get_metadata(path)
        return isinstance(meta, dropbox_api.files.FolderMetadata)
    except dropbox_api.exceptions.ApiError:
        return False


def rename_file(dbx, src_path, dst_path):
    """Rename a file from one path to another."""
    if is_file(dbx, src_path):
        print('Renaming file: {} to {}'.format(src_path, dst_path))
        dbx.files_move_v2(src_path, dst_path)
    else:
        raise ValueError('Cannot rename a file that does not exist.')


def download_dropbox_file(dbx, dropbox_path, local_path):
    dbx.files_download_to_file(local_path, dropbox_path)


def normalise_path(path):
    """Modify a path (str or Path) such that it is a Dropbox-compatible path string."""
    path = posixpath.join(*str(path).split(os.path.sep))
    if not path.startswith('/'):
        path = '/' + path
    return path


def archive_file(dbx, local_path, dropbox_dir, check_modified_time=True):
    """Upload a file to a dropbox directory such that if the local file is newer than the
    copy on Dropbox, the newer file overwrites the older file, and if the local file is
    older than the copy on Dropbox, the local file is uploaded with an "auto-incremented"
    name.

    Note that if the modified times are different, the file still won't be uploaded
    by dropbox if the file contents are identical. The point of checking for the
    modified times is to save some time by not having Dropbox check file contents.    

    Parameters
    ----------
    dbx: Dropbox
    local_path : str or Path
        Path of file on local computer to upload to dropbox.
    dropbox_dir : str or Path
        Directory on Dropbox into which the file should be uploaded.

    """

    local_path = Path(local_path)
    dropbox_path = normalise_path(Path(dropbox_dir).joinpath(local_path.name))

    if not check_modified_time:
        overwrite = True
        autorename = False
        client_modified = None

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
            existing_file = dbx.files_get_metadata(dropbox_path)
            existing_modified = existing_file.client_modified

            # print('client_modified: {!r}'.format(client_modified))
            # print('existing_modified: {!r}'.format(existing_modified))

            if client_modified == existing_modified:
                # print('client_modified time same!')
                return

            elif client_modified < existing_modified:
                overwrite = False
                autorename = True
                # print('client file older!')

            elif client_modified > existing_modified:
                overwrite = True
                autorename = False
                # print('client file newer!')

        except dropbox_api.exceptions.ApiError:

            # print('File does not exist.')

            overwrite = False
            autorename = False

        except:
            msg = 'Unexpected error.'
            raise CloudProviderError(msg)

    _upload_dropbox_file(
        dbx,
        local_path,
        dropbox_path,
        overwrite=overwrite,
        autorename=autorename,
        client_modified=client_modified
    )


def upload_dropbox_file(dbx, local_path, dropbox_dir, overwrite=False, autorename=True,
                        client_modified=None):
    """
    Parameters
    ----------
    dbx: Dropbox
    local_path : str or Path
        Path of file on local computer to upload to dropbox.
    dropbox_dir : str or Path
        Directory on Dropbox into which the file should be uploaded.
    overwrite : bool
        If True, the file overwrites an existing file with the same name.
    autorename : bool
        If True, rename the file if there is a conflict.

    """

    local_path = Path(local_path)
    dropbox_path = normalise_path(Path(dropbox_dir).joinpath(local_path.name))
    _upload_dropbox_file(
        dbx,
        local_path,
        dropbox_path,
        overwrite=overwrite,
        autorename=autorename,
        client_modified=client_modified,
    )


def _upload_dropbox_file(dbx, local_path, dropbox_path, overwrite=False, autorename=True,
                         client_modified=None):
    """
    Parameters
    ----------
    dbx: Dropbox
    local_path : Path
        Path of file on local computer to upload to dropbox.
    dropbox_path : str
        Path to upload the file to nn dropbox.
    overwrite : bool
        If True, the file overwrites an existing file with the same name.
    autorename : bool
        If True, rename the file if there is a conflict.

    """

    if overwrite:
        mode = dropbox_api.dropbox.files.WriteMode('overwrite', None)
    else:
        mode = dropbox_api.dropbox.files.WriteMode('add', None)

    try:
        with local_path.open(mode='rb') as handle:

            try:
                dbx.files_upload(
                    handle.read(),
                    dropbox_path,
                    mode=mode,
                    autorename=autorename,
                    client_modified=client_modified,
                )

            except dropbox_api.exceptions.ApiError as err:
                msg = ('Cloud provider error. {}'.format(err))
                raise CloudProviderError(msg)
            except:
                msg = 'Unexpected error.'
                raise CloudProviderError(msg)

    except FileNotFoundError as err:
        raise ArchiveError(err)


def archive_directory(dbx, local_dir, dropbox_dir, exclude=None):
    """
    Archive a the contents of a local directory into a directory on dropbox.

    Any files in the dropbox directory not in the source directory are ignored.

    Parameters
    ----------
    dbx: Dropbox
    local_dir : str or Path
        Path of directory on local computer to upload to dropbox.
    dropbox_dir : str or Path
        Directory on dropbox to upload the files to.

    """

    print('hpcflow.archive.cloud.providers.dropbox.archive_directory', flush=True)

    local_dir = Path(local_dir)
    dropbox_dir = Path(dropbox_dir)
    _upload_dropbox_dir(
        dbx,
        local_dir,
        dropbox_dir,
        exclude=exclude,
        archive=True,
    )


def upload_dropbox_dir(dbx, local_dir, dropbox_dir, overwrite=False, autorename=False,
                       exclude=None):
    """
    Parameters
    ----------
    dbx: Dropbox
    local_dir : str or Path
        Path of directory on local computer to upload to dropbox.
    dropbox_dir : str or Path
        Directory on dropbox to upload the file to.
    overwrite : bool
        If True, the file overwrites an existing file with the same name.
    autorename : bool
        If True, rename the file if there is a conflict.
    exclude : list, optional
        List of file or directory names to exclude, matched with `fnmatch` for
        files, or compared directly for directories.

    """

    local_dir = Path(local_dir)
    dropbox_dir = Path(dropbox_dir)
    _upload_dropbox_dir(
        dbx,
        local_dir,
        dropbox_dir,
        overwrite=overwrite,
        autorename=autorename,
        exclude=exclude,
        archive=False,
    )


def _upload_dropbox_dir(dbx, local_dir, dropbox_dir, overwrite=False, autorename=False,
                        exclude=None, archive=False):
    """
    Parameters
    ----------
    dbx: Dropbox
    local_dir : Path
        Path of directory on local computer to upload to dropbox.
    dropbox_dir : Path
        Directory on dropbox to upload the file to.
    overwrite : bool
        If True, the file overwrites an existing file with the same name.
    autorename : bool
        If True, rename the file if there is a conflict.
    exclude : list, optional
        List of file or directory names to exclude, matched with `fnmatch` for
        files, or compared directly for directories.
    archive : bool, optional

    Notes
    -----
    Does not upload empty directories.

    """

    if not exclude:
        exclude = []

    # Validation
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
                        archive_file(
                            dbx,
                            src_file,
                            dst_dir
                        )
                    else:
                        upload_dropbox_file(
                            dbx,
                            src_file,
                            dst_dir,
                            overwrite=overwrite,
                            autorename=autorename
                        )
                except ArchiveError as err:
                    print('Archive error: {}'.format(err), flush=True)
                    continue

                except CloudProviderError as err:
                    print('Cloud provider error: {}'.format(err), flush=True)
                    continue
