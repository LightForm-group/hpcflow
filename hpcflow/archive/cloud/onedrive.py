import atexit
import json
import os
from pathlib import Path
from datetime import datetime

from typing import Union, List

import msal as msal
import requests

from hpcflow.archive.cloud.cloud import CloudProvider
from hpcflow.config import Config

# This is the file size limit in bytes that triggers a session upload instead
# of a simple upload.
CHUNK_SIZE = 4 * 1024 * 1024


class OnedriveCloudProvider(CloudProvider):
    """A OnedriveCloudProvider provides methods for archiving directories and their
    contents to Dropbox."""
    def __init__(self):
        self.token_scope = ["User.Read"]
        self.cache_name = "cache.bin"

    def get_token_cache(self) -> msal.SerializableTokenCache:
        """Attempt to load a TokenCache from a file. If the file does not exist then return an empty
        TokenCache."""
        cache = msal.SerializableTokenCache()
        if os.path.exists(self.cache_name):
            cache.deserialize(open(self.cache_name, "r").read())
        return cache

    def get_access_token(self) -> str:
        """Get an API access token using the Microsoft Authentication Library. First try to get a
        token silently using a local token cache. If this doesn't work use the device flow workflow
        to get a token."""
        token_cache = self.get_token_cache()
        app = msal.PublicClientApplication(Config.get('onedrive_client_id'),
                                           authority="https://login.microsoftonline.com/common",
                                           token_cache=token_cache)

        # Register token cache to be saved on program exit.
        atexit.register(lambda: open("cache.bin", "w").write(token_cache.serialize()))

        token = self.get_token_from_cache(app, self.token_scope)
        if token:
            return token
        else:
            return self.get_token_by_device_flow(app, self.token_scope)

    @staticmethod
    def get_token_from_cache(app: msal.PublicClientApplication,
                             token_scope: List[str]) -> Union[str, None]:
        """Try to get an API access token from a local cache. If the access token is expired
         a refresh token will automatically be used to get a new access token. If the refresh token
         is expired then the user will have to reauthenticate."""
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(token_scope, account=accounts[0])
            # Method returns None if no token can be acquired
            if result and "access_token" in result:
                return result["access_token"]
            else:
                return None

    @staticmethod
    def get_token_by_device_flow(app: msal.PublicClientApplication, token_scope: List[str]) -> str:
        flow = app.initiate_device_flow(scopes=token_scope)
        if "user_code" not in flow:
            raise ValueError("Fail to create device flow. Err: %s" % json.dumps(flow, indent=4))
        print(flow["message"])
        print("Program execution will continue automatically after authentication.")

        # This function polls every 5 seconds to see if the user has completed the authentication
        result = app.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            return result["access_token"]
        else:
            print(result.get("error"))
            print(result.get("error_description"))
            raise ValueError()

    def check_access(self) -> bool:
        """Check whether the supplied access token is valid for making a connection to the
        Microsoft Graph API."""
        header = {'Authorization': 'Bearer ' + self.get_access_token()}
        try:
            profile_data = requests.get("https://graph.microsoft.com/v1.0/me",
                                        headers=header).json()
        except Exception:
            # This can be an error from requests due to some sort of connection problem
            return False
        if "displayName" in profile_data:
            return True
        else:
            # This is due to some sort mangled response
            return False

    def get_directories(self, path: Union[str, Path]) -> List[str]:
        """Get a list of sub directories within a dropbox path.

        Parameters
        ----------
        path
            The dropbox path to list the directories of. Path is relative to the Dropbox root.
        """

    def check_directory_exists(self, directory: Union[str, Path]) -> bool:
        """Check a given directory exists on dropbox.

        Parameters
        ----------
        directory
            The directory on Dropbox to check. Path is relative to the Dropbox root.
        """

    def _get_dropbox_file_modified_time(self, dropbox_path: str) -> Union[datetime, None]:
        """Get the last modified time of a file on Dropbox. Returns None if the file
        does not exist.

        Parameters
        ----------
        dropbox_path
            The path of a file on Dropbox to check. Path is relative to the Dropbox root.
        """

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


def get_auth_code() -> str:
    """In order to connect to dropbox HPCFlow must be authenticated with Dropbox as a valid app.
    This is done by the user getting an authorisation token from a url and providing it to hpcflow.
    hpcflow then uses this auth token to get an API token from Dropbox.

    This function prompts the user to go to the auth token url, accepts it as input and gets and
    returns the subsequent API token.
    """
