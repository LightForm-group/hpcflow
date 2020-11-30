import datetime
from pathlib import Path
from unittest.mock import patch, Mock

import pytest
import dropbox
import dropbox.exceptions
import dropbox.files

from hpcflow.archive.cloud.dropbox_cp import DropboxCloudProvider
import hpcflow.archive.cloud.dropbox_cp as dropbox_cp
from hpcflow.archive.cloud.errors import CloudProviderError, CloudCredentialsError
from hpcflow.archive.errors import ArchiveError
from hpcflow.config import Config
from hpcflow.errors import ConfigurationError


@pytest.fixture(scope="session")
def set_config():
    Config.set_config()


@pytest.fixture(scope="session")
def cloud_provider() -> DropboxCloudProvider:
    return DropboxCloudProvider()


class MockResult:
    result = ""


mock_file = Mock(spec=dropbox.files.FileMetadata)
mock_file.name = "Sample File"

mock_folder = Mock(spec=dropbox.files.FolderMetadata)
mock_folder.name = "Sample Folder"


class MockFolderList:
    entries = [mock_file, mock_folder]


class TestGetToken:
    """Test getting the Dropbox API token without setting up the config first."""
    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_token_provided(self, mock_dropbox):
        _ = DropboxCloudProvider("aaa")
        mock_dropbox.assert_called_with("aaa")

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_env_var_token(self, mock_dropbox, mock_get_env):
        mock_get_env.return_value = "aaa"
        DropboxCloudProvider()
        mock_dropbox.assert_called_with("aaa")

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    def test_config_not_initialised(self, mock_get_env):
        mock_get_env.return_value = None
        with pytest.raises(ConfigurationError):
            DropboxCloudProvider()

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    @patch('hpcflow.archive.cloud.dropbox_cp.Config.get')
    def test_config_value_none(self, mock_config_get, mock_get_env):
        mock_get_env.return_value = None
        mock_config_get.return_value = None
        with pytest.raises(CloudCredentialsError):
            DropboxCloudProvider()


@pytest.mark.usefixtures("set_config")
class TestDropboxCloudProvider:
    """Tests for most of the methods in DropboxCloudProvider."""
    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_init_type(self, mock_dropbox, cloud_provider):
        assert isinstance(cloud_provider, DropboxCloudProvider)

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.check_user')
    def test_check_valid_access(self, mock_check_user, cloud_provider):
        mock_check_user.return_value = Mock(result="")
        assert cloud_provider.check_access() is True

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.check_user')
    def test_check_invalid_access(self, mock_check_user, cloud_provider):
        mock_check_user.side_effect = dropbox.exceptions.AuthError("", "")
        assert cloud_provider.check_access() is False

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_list_folder')
    def test_check_directories(self, mock_files_list_folder, cloud_provider):
        mock_files_list_folder.return_value = MockFolderList
        file_list = cloud_provider.get_directories(".")
        assert file_list == ["Sample Folder"]

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_nonexistent_directory(self, mock_files_get_metadata, cloud_provider):
        mock_files_get_metadata.return_value = mock_file
        assert cloud_provider.check_directory_exists("Sample Folder") is False

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_directory_error(self, mock_files_get_metadata, cloud_provider):
        """Test for API error when checking if directory exists."""
        mock_files_get_metadata.side_effect = dropbox.exceptions.ApiError("", "", "", "")
        with pytest.raises(CloudProviderError):
            cloud_provider.check_directory_exists("Sample Folder")

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_extant_directory(self, mock_files_get_metadata, cloud_provider):
        mock_files_get_metadata.return_value = mock_folder
        assert cloud_provider.check_directory_exists("Sample Folder") is True

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_get_dropbox_modified_time(self, mock_get_metadata, cloud_provider):
        mock_get_metadata.return_value = Mock(spec=dropbox.files.FileMetadata,
                                              client_modified=datetime.datetime.now())
        time = cloud_provider._get_dropbox_file_modified_time("")
        assert isinstance(time, datetime.datetime)

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_failed_get_dropbox_modified_time(self, mock_get_metadata, cloud_provider):
        mock_get_metadata.return_value = Mock(spec=dropbox.files.FolderMetadata)
        with pytest.raises(ArchiveError):
            cloud_provider._get_dropbox_file_modified_time("")

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_upload')
    @patch('hpcflow.archive.cloud.dropbox_cp.cloud.read_file_contents')
    def test_upload_file_to_dropbox(self, mock_read_file, mock_file_upload,
                                    cloud_provider: DropboxCloudProvider):
        """This simulates a successful upload to dropbox."""
        mock_file_upload.return_value = Mock(spec=dropbox.files.FileMetadata)
        mock_read_file.return_value = b"Sample text."
        file_metadata = cloud_provider._upload_file_to_dropbox("", "")
        assert isinstance(file_metadata, dropbox.files.FileMetadata)

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_upload')
    @patch('hpcflow.archive.cloud.dropbox_cp.cloud.read_file_contents')
    def test_failed_upload_file_to_dropbox(self, mock_read_file, mock_file_upload,
                                           cloud_provider: DropboxCloudProvider):
        """This simulates a failed upload to dropbox due to an API error."""
        mock_read_file.return_value = b"Sample text."
        mock_file_upload.side_effect = dropbox.exceptions.ApiError("", "", "", "")
        with pytest.raises(CloudProviderError):
            cloud_provider._upload_file_to_dropbox("", "")


class TestDropboxArchiveDirectory:
    """Tests for the main function of DropboxCloudProvider - uploading of a directory."""
    @patch('hpcflow.archive.cloud.dropbox_cp.Path.is_dir')
    def test_upload_file(self, mock_is_dir, cloud_provider):
        mock_is_dir.return_value = False
        with pytest.raises(ValueError):
            cloud_provider.archive_directory("a_file_name.txt", ".")

    @patch('hpcflow.archive.cloud.dropbox_cp.Path.is_dir')
    @patch('hpcflow.archive.cloud.dropbox_cp.os.walk')
    @patch('hpcflow.archive.cloud.dropbox_cp.DropboxCloudProvider._archive_file')
    def test_upload_directory(self, mock_archive_file, mock_os_walk, mock_is_dir, cloud_provider):
        mock_is_dir.return_value = True
        mock_os_walk.return_value = (("C:/upload", ["nested"], ["a.txt", "b.txt"]),
                                     ("C:/upload/nested", [], ["c.txt"], ))
        cloud_provider.archive_directory("C:/upload", "dest_folder")
        mock_archive_file.assert_any_call(Path("C:/upload/a.txt"), Path("dest_folder"))
        mock_archive_file.assert_any_call(Path("C:/upload/b.txt"), Path("dest_folder"))
        mock_archive_file.assert_any_call(Path("C:/upload/nested/c.txt"),
                                          Path("dest_folder/nested"))


class TestStaticMethods:
    """Tests for methods in the dropbox_cp file that are not associated with the
    DropboxCloudProvider object"""

    def test_normalise_path(self):
        assert dropbox_cp._normalise_path(".") == ""
        assert dropbox_cp._normalise_path(r"Documents\Fred") == "/Documents/Fred"
        assert dropbox_cp._normalise_path("Documents/Fred") == "/Documents/Fred"
        assert dropbox_cp._normalise_path(Path(".")) == ""
        assert dropbox_cp._normalise_path(Path(r"Documents\Fred")) == "/Documents/Fred"
        assert dropbox_cp._normalise_path(Path("Documents/Fred")) == "/Documents/Fred"

    @patch('hpcflow.archive.cloud.dropbox_cp.Path.stat')
    def test_client_modified_time(self, mock_stat_function):
        mock_stat_function.return_value = Mock(st_mtime=1606327099)
        modified_time = dropbox_cp._get_client_modified_time(Path(__file__))
        assert isinstance(modified_time, datetime.datetime)
        assert modified_time == datetime.datetime(year=2020, month=11, day=25, hour=17, minute=58,
                                                  second=19)
