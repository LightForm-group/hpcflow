"""`hpcflow.archive.cloud.cloud.py"""

from abc import ABC


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
