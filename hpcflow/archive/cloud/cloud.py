"""`hpcflow.archive.cloud.cloud.py"""
import fnmatch
from abc import ABC
from pathlib import Path
from typing import List

from hpcflow.archive.errors import ArchiveError


class CloudProvider(ABC):
    """A cloud provider is a service that allows the archival of files to a remote location."""
    def check_access(self) -> bool:
        """Check if there is a valid connection to the cloud provider. Return Tre if there is
        a valid connection else return False."""
        raise NotImplementedError("This method is not implemented in the base class.")

    def archive_directory(self, local_path, remote_path, exclude):
        raise NotImplementedError("This method is not implemented in the base class.")

    def get_directories(self, path):
        """Get a list of sub directories within a path"""
        raise NotImplementedError("This method is not implemented in the base class.")

    def check_directory_exists(self, directory):
        """Check a given directory exists on the cloud storage."""
        raise NotImplementedError("This method is not implemented in the base class.")


def exclude_specified_files(files: List[str], exclude: List[str]) -> List[str]:
    """Remove any file from `files` that matches a pattern in `exclude`."""
    files_to_exclude = [fnmatch.filter(files, pattern) for pattern in exclude]
    # Flatten list of lists
    files_to_exclude = [item for sublist in files_to_exclude for item in sublist]
    return list(set(files) - set(files_to_exclude))


def read_file_contents(path: Path) -> bytes:
    """Try to read the file at `path` in binary format and return its contents. """
    try:
        with path.open(mode='rb') as handle:
            return handle.read()
    except FileNotFoundError as err:
        raise ArchiveError(err)


def exclude_specified_directories(dirs: List[str], exclude: List[str]):
    """Remove any string from dirs that occurs in exclude."""
    return [dir_name for dir_name in dirs if dir_name not in exclude]