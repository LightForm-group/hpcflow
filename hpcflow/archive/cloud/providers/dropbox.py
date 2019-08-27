"""`hpcflow.archive.cloud.providers.dropbox.py`

This module provides interactivity with a Dropbox account for archiving.

"""

import os
import posixpath
import fnmatch
from pathlib import Path
from pprint import pprint

import dropbox as dropbox_api

from hpcflow.archive.errors import ArchiveError
from hpcflow.archive.cloud.errors import (CloudProviderError,
                                          CloudCredentialsError)


def get_dropbox():

    print('hpcflow.archive.cloud.providers.dropbox.get_dropbox', flush=True)

    env_var_name = 'HPCFLOW_CLOUD_TOKEN_DROPBOX'
    token = os.getenv(env_var_name)
    if not token:
        msg = ('Please set the Dropbox access token in an environment variable'
               ' called `{}`.'.format(env_var_name))
        raise CloudCredentialsError(msg)

    dbx = dropbox_api.Dropbox(token)

    return dbx


def check_access():

    print('hpcflow.archive.cloud.providers.dropbox.check_access', flush=True)
    dbx = get_dropbox()
    try:
        user_acc = dbx.users_get_current_account()
    except:
        msg = 'Could not connect to Dropbox using the supplied access token.'
        raise ValueError(msg)

    print('Successfully connected to Dropbox.', flush=True)


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
    'Modify a path (str or Path) such that it is a Dropbox-compatible path string.'
    path = posixpath.join(*str(path).split(os.path.sep))
    if not path.startswith('/'):
        path = '/' + path
    return path


def upload_dropbox_file(dbx, local_path, dropbox_dir, overwrite=False,
                        autorename=False):
    """
    Parameters
    ----------
    dbx: Dropbox
    local_path : str or Path
        Path of file on local computer to upload to dropbox.
    dropbox_dir : str or Path
        Directory on dropbox to upload the file to.
    overwrite : bool
        If True, the file overwrites an existing file with the same name.
    autorename : bool
        If True, rename the file if there is a conflict.

    """

    local_path = Path(local_path)
    dropbox_path = normalise_path(Path(dropbox_dir).joinpath(local_path.name))

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
                    autorename=autorename
                )

            except dropbox_api.exceptions.ApiError as err:
                msg = ('Cloud provider error. {}'.format(err))
                raise CloudProviderError(msg)
            except:
                msg = 'Unexpected error.'
                raise CloudProviderError(msg)

    except FileNotFoundError as err:
        raise ArchiveError(err)


def upload_dropbox_dir(dbx, local_path, dropbox_path, overwrite=False,
                       autorename=False, exclude=None):
    """
    Parameters
    ----------
    dbx: Dropbox
    local_path : str
        Path of file on local computer to upload to dropbox.
    dropbox_path : str
        Directory on dropbox to upload the file to.
    overwrite : bool
        If True, the file overwrites an existing file with the same name.
    autorename : bool
        If True, rename the file if there is a conflict.
    exclude : list, optional
        List of file or directory names to exclude, matched with `fnmatch` for
        files, or compared directly for directories.

    Notes
    -----
    Does not upload empty directories.

    """

    print('hpcflow.archive.cloud.providers.dropbox.upload_dropbox_dir', flush=True)

    if not exclude:
        exclude = []

    # Validation
    if not os.path.isdir(local_path):
        raise ValueError(
            'Specified `local_path` is not a directory: {}'.format(local_path))

    local_path = Path(local_path)
    dropbox_path = Path(dropbox_path)

    for root, dirs, files in os.walk(local_path):

        root_test = Path(root)

        dirs[:] = [d for d in dirs if d not in exclude]

        print('Uploading from root directory: {}'.format(root), flush=True)

        for file_name in files:

            up_file = False

            if exclude is not None:
                if not any([fnmatch.fnmatch(file_name, i) for i in exclude]):
                    up_file = True
            else:
                up_file = True

            if up_file:

                src_file = root_test.joinpath(file_name)
                rel_path = src_file.relative_to(local_path)
                dst_dir = dropbox_path.joinpath(rel_path.parent)

                print('Uploading file: {}'.format(file_name), flush=True)
                try:
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
