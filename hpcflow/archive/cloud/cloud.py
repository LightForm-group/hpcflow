"""`hpcflow.archive.cloud.cloud.py"""
import fnmatch
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Union

from hpcflow.archive.errors import ArchiveError


class CloudProvider(ABC):
    """A cloud provider is a service that allows the archival of files to a remote location."""
    @abstractmethod
    def check_access(self) -> bool:
        """Check if there is a valid connection to the cloud provider. Return Tre if there is
        a valid connection else return False."""

    @abstractmethod
    def archive_directory(self, local_path: Union[str, Path], remote_path: Union[str, Path],
                          exclude: List[str]):
        """Archive the contents of a local directory to a remote provider."""

    @abstractmethod
    def get_directories(self, path: Union[str, Path]) -> List[str]:
        """Get a list of sub directories within a path"""

    @abstractmethod
    def check_directory_exists(self, directory: Union[str, Path]):
        """Check a given directory exists on the cloud storage."""


def exclude_specified_files(files: List[str], exclude: List[str]) -> List[str]:
    """Remove any file from `files` that matches a pattern in `exclude`."""
    files_to_exclude = [fnmatch.filter(files, pattern) for pattern in exclude]
    # Flatten list of lists
    files_to_exclude = [item for sublist in files_to_exclude for item in sublist]
    return list(set(files) - set(files_to_exclude))


def read_file_contents(path: Path) -> bytes:
    """Try to read the local file at `path` in binary format and return its contents. """
    try:
        with path.open(mode='rb') as handle:
            return handle.read()
    except FileNotFoundError as err:
        raise ArchiveError(err)


def exclude_specified_directories(dirs: List[str], exclude: List[str]):
    """Remove any string from dirs that occurs in exclude."""
    return [dir_name for dir_name in dirs if dir_name not in exclude]