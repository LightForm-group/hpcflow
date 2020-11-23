from unittest.mock import patch, Mock

import pytest
import dropbox
import dropbox.exceptions
import dropbox.files

from hpcflow.archive.cloud.dropbox_cp import DropboxCloudProvider
from hpcflow.config import Config


@pytest.fixture(scope="session")
def set_config():
    Config.set_config()


class MockResult:
    result = ""


mock_file = Mock(spec=dropbox.files.FileMetadata)
mock_file.name = "Sample File"

mock_folder = Mock(spec=dropbox.files.FolderMetadata)
mock_folder.name = "Sample Folder"


class MockFolderList:
    entries = [mock_file, mock_folder]


@pytest.mark.usefixtures("set_config")
class TestDropboxCloudProvider:

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_init_type(self, mock_dropbox):
        cloud_provider = DropboxCloudProvider()
        assert isinstance(cloud_provider, DropboxCloudProvider)

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_token_provided(self, mock_dropbox):
        _ = DropboxCloudProvider("aaa")
        assert mock_dropbox.called_with("aaa")

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.check_user')
    def test_check_valid_access(self, mock_check_user: DropboxCloudProvider):
        mock_check_user.return_value = Mock(result="")
        cloud_provider = DropboxCloudProvider()
        assert cloud_provider.check_access() is True

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.check_user')
    def test_check_invalid_access(self, mock_check_user):
        mock_check_user.get.side_effects = dropbox.exceptions.AuthError
        cloud_provider = DropboxCloudProvider()
        assert cloud_provider.check_access() is False

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_list_folder')
    def test_check_directories(self, mock_files_list_folder):
        mock_files_list_folder.return_value = MockFolderList
        cloud_provider = DropboxCloudProvider()
        file_list = cloud_provider.get_directories(".")
        assert file_list == ["Sample Folder"]

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_nonexistent_directory(self, mock_files_get_metadata):
        mock_files_get_metadata.return_value = mock_file
        cloud_provider = DropboxCloudProvider()
        assert cloud_provider.check_directory_exists("Sample Folder") is False

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_extant_directory(self, mock_files_get_metadata):
        mock_files_get_metadata.return_value = mock_folder
        cloud_provider = DropboxCloudProvider()
        assert cloud_provider.check_directory_exists("Sample Folder") is True
