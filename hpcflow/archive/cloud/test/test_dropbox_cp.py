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
from hpcflow.errors import ConfigurationError


@pytest.fixture(scope="session")
def cloud_provider() -> DropboxCloudProvider:
    """Initialise a CloudProvider fixture to use for the tests. It doesn't matter that we
    initialise the Fixture with a fake token as authentication isn't done until needed
    and we patch all methods that might trigger authentication."""
    return DropboxCloudProvider("aaa")


# These mock objects simulate the return type of a Dropbox action on a file or folder.
mock_file = Mock(spec=dropbox.files.FileMetadata)
mock_file.name = "Sample File"
mock_folder = Mock(spec=dropbox.files.FolderMetadata)
mock_folder.name = "Sample Folder"


class TestDropboxCloudProviderInit:
    """Tests for initialisation of DropboxCloudProvider and the ways a token can be given."""
    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_provided_token_init(self, mock_dropbox):
        """Init is called with a token."""
        DropboxCloudProvider("aaa")
        mock_dropbox.assert_called_with("aaa")

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_env_var_token_init(self, mock_dropbox, mock_get_env):
        """The token is stored as an environment variable."""
        mock_get_env.return_value = "aaa"
        DropboxCloudProvider()
        mock_dropbox.assert_called_with("aaa")

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    def test_config_not_initialised(self, mock_get_env):
        """There is no environment variable and the config has not been initialised."""
        mock_get_env.return_value = None
        with pytest.raises(ConfigurationError):
            DropboxCloudProvider()

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    @patch('hpcflow.archive.cloud.dropbox_cp.Config.get')
    def test_config_value_none(self, mock_config_get, mock_get_env):
        """There is no environment variable, config is initialised but contains no value
        for dropbox."""
        mock_get_env.return_value = None
        mock_config_get.return_value = None
        with pytest.raises(CloudCredentialsError):
            DropboxCloudProvider()

    @patch('hpcflow.archive.cloud.dropbox_cp.os.getenv')
    @patch('hpcflow.archive.cloud.dropbox_cp.Config.get')
    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox')
    def test_config_value(self, mock_dropbox, mock_config_get, mock_get_env):
        """There is no environment variable, config is initialised and returns a token."""
        mock_get_env.return_value = None
        mock_config_get.return_value = "aaa"
        DropboxCloudProvider()
        mock_dropbox.assert_called_with("aaa")


class TestDropboxCloudProviderStatus:
    """These tests cover functions that interrogate dropbox for information. This includes
    whether the use is valid and directory listing."""
    def test_init_type(self, cloud_provider):
        """For the rest of the tests we use the `cloud_provider` fixture
        to initialise a DropboxCloudProvider."""
        assert isinstance(cloud_provider, DropboxCloudProvider)

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.check_user')
    def test_check_access_valid(self, mock_check_user, cloud_provider):
        """Simulate the check access function if token is valid."""
        mock_check_user.return_value = Mock(result="")
        assert cloud_provider.check_access() is True

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.check_user')
    def test_check_access_invalid(self, mock_check_user, cloud_provider):
        """Simulate the check access function if token is invalid."""
        mock_check_user.side_effect = dropbox.exceptions.AuthError("", "")
        assert cloud_provider.check_access() is False

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_list_folder')
    def test_check_directories(self, mock_files_list_folder, cloud_provider):
        """Use the get_directories function to return the contents of a folder."""
        mock_files_list_folder.return_value = Mock(entries=[mock_file, mock_folder])
        file_list = cloud_provider.get_directories(".")
        assert file_list == ["Sample Folder"]

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_nonexistent_directory(self, mock_files_get_metadata, cloud_provider):
        """Simulate use of the the check_directory_exists function on a non existent folder."""
        mock_files_get_metadata.return_value = mock_file
        assert cloud_provider.check_directory_exists("Sample Folder") is False

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_directory_error(self, mock_files_get_metadata, cloud_provider):
        """Simulate use of the the check_directory_exists function on something that
        isn't a directory."""
        mock_files_get_metadata.side_effect = dropbox.exceptions.ApiError("", "", "", "")
        with pytest.raises(CloudProviderError):
            cloud_provider.check_directory_exists("Sample Folder")

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_check_extant_directory(self, mock_files_get_metadata, cloud_provider):
        """Simulate use of the the check_directory_exists function on a folder that exists."""
        mock_files_get_metadata.return_value = mock_folder
        assert cloud_provider.check_directory_exists("Sample Folder") is True

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_get_dropbox_modified_time(self, mock_get_metadata, cloud_provider):
        """Get the modified time of a file on dropbox."""
        mock_get_metadata.return_value = Mock(spec=dropbox.files.FileMetadata,
                                              client_modified=datetime.datetime.now())
        time = cloud_provider._get_dropbox_file_modified_time("")
        assert isinstance(time, datetime.datetime)
        assert time - datetime.datetime.now() < datetime.timedelta(seconds=10)

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_get_metadata')
    def test_failed_get_dropbox_modified_time(self, mock_get_metadata, cloud_provider):
        """Try to get the modified time of something that isn't a file."""
        mock_get_metadata.return_value = Mock(spec=dropbox.files.FolderMetadata)
        with pytest.raises(ArchiveError):
            cloud_provider._get_dropbox_file_modified_time("")


class TestDropboxCloudProviderUpload:
    """These tests cover the file upload functions of DropboxCloudProvider."""
    @patch('hpcflow.archive.cloud.dropbox_cp.Path.is_dir')
    @patch('hpcflow.archive.cloud.dropbox_cp.generate_files')
    @patch('hpcflow.archive.cloud.dropbox_cp.DropboxCloudProvider._archive_file')
    def test_upload_directory(self, mock_archive_file, mock_generate_files,
                              mock_is_dir, cloud_provider):
        """Upload a the contents of a directory"""
        mock_is_dir.return_value = True
        mock_generate_files.return_value = {"C:/upload": ["a.txt", "b.txt"],
                                            "C:/upload/nested": ["c.txt"]}
        cloud_provider.archive_directory("C:/upload", "dest_folder")
        mock_archive_file.assert_any_call(Path("C:/upload/a.txt"), Path("dest_folder"))
        mock_archive_file.assert_any_call(Path("C:/upload/b.txt"), Path("dest_folder"))
        mock_archive_file.assert_any_call(Path("C:/upload/nested/c.txt"),
                                          Path("dest_folder/nested"))

    @patch('hpcflow.archive.cloud.dropbox_cp.Path.is_dir')
    def test_upload_file(self, mock_is_dir, cloud_provider):
        """Try to upload a file instead of a directory."""
        mock_is_dir.return_value = False
        with pytest.raises(ValueError):
            cloud_provider.archive_directory("a_file_name.txt", ".")

# Tests for _archive_file

    @patch('hpcflow.archive.cloud.dropbox_cp.DropboxCloudProvider.simple_upload')
    @patch('hpcflow.archive.cloud.dropbox_cp.cloud.read_file_contents')
    @patch('hpcflow.archive.cloud.dropbox_cp.os.path.getsize')
    def test_failed_upload_file_to_dropbox(self, mock_get_size, mock_read_file, mock_file_upload,
                                           cloud_provider: DropboxCloudProvider):
        """This simulates a failed upload to dropbox due to an API error."""
        mock_get_size.return_value = 1000
        mock_read_file.return_value = b"Sample text."
        mock_file_upload.side_effect = dropbox.exceptions.ApiError("", "", "", "")
        with pytest.raises(CloudProviderError):
            cloud_provider._upload_file_to_dropbox("", "")

    @patch('hpcflow.archive.cloud.dropbox_cp.os.path.getsize')
    @patch('hpcflow.archive.cloud.dropbox_cp.DropboxCloudProvider.simple_upload')
    def test_upload_small_file(self, mock_simple_upload, mock_getsize, cloud_provider):
        mock_getsize.return_value = 1000
        cloud_provider._select_upload_method("", "", "", "")
        assert mock_getsize.return_value < dropbox_cp.CHUNK_SIZE
        mock_simple_upload.assert_called()

    @patch('hpcflow.archive.cloud.dropbox_cp.os.path.getsize')
    @patch('hpcflow.archive.cloud.dropbox_cp.DropboxCloudProvider.session_upload')
    def test_upload_big_file(self, mock_session_upload, mock_getsize, cloud_provider):
        mock_getsize.return_value = 10000000
        cloud_provider._select_upload_method("", "", "", "")
        assert mock_getsize.return_value > dropbox_cp.CHUNK_SIZE
        mock_session_upload.assert_called()

# session upload

    @patch('hpcflow.archive.cloud.dropbox_cp.dropbox.Dropbox.files_upload')
    @patch('hpcflow.archive.cloud.dropbox_cp.cloud.read_file_contents')
    def test_simple_upload(self, mock_read_file, mock_file_upload,
                           cloud_provider: DropboxCloudProvider):
        """This simulates a successful upload to dropbox."""
        mock_file_upload.return_value = Mock(spec=dropbox.files.FileMetadata)
        mock_read_file.return_value = b"Sample text."
        overwrite = dropbox.dropbox.files.WriteMode('overwrite')
        file_metadata = cloud_provider.simple_upload(Path(""), "", overwrite, None)
        assert isinstance(file_metadata, dropbox.files.FileMetadata)


class TestStaticMethods:
    """Tests for methods in the dropbox_cp file that are not associated with the
    DropboxCloudProvider object"""
    @patch('hpcflow.archive.cloud.dropbox_cp.os.walk')
    def test_generate_files(self, mock_os_walk):
        """Generate a dictionary representation of files to be uploaded using `os.walk`."""
        mock_os_walk.return_value = (("C:/upload", ["nested"], ["a.txt", "b.txt"]),
                                     ("C:/upload/nested", [], ["c.txt"]))
        file_list = dropbox_cp.generate_files(Path("C:/upload"))
        assert file_list == {"C:/upload": ["a.txt", "b.txt"],
                             "C:/upload/nested": ["c.txt"]}

    @patch('hpcflow.archive.cloud.dropbox_cp.os.walk')
    def test_generate_files_exclude_files(self, mock_os_walk):
        """Generate a dictionary of files to be uploaded, excluding a file using
        pattern matching."""
        mock_os_walk.return_value = (("C:/upload", ["nested"], ["a.txt", "b.txt"]),
                                     ("C:/upload/nested", [], ["c.txt"]))
        file_list = dropbox_cp.generate_files(Path("C:/upload"), ["b*"])
        assert file_list == {"C:/upload": ["a.txt"],
                             "C:/upload/nested": ["c.txt"]}

    @patch('hpcflow.archive.cloud.dropbox_cp.Path.stat')
    def test_client_modified_time(self, mock_stat_function):
        """Simulate getting the modified time of a local file and converting to Dropbox format."""
        mock_stat_function.return_value = Mock(st_mtime=1606327099)
        modified_time = dropbox_cp._get_client_modified_time(Path(__file__))
        assert isinstance(modified_time, datetime.datetime)
        assert modified_time == datetime.datetime(year=2020, month=11, day=25, hour=17, minute=58,
                                                  second=19)

    def test_normalise_path(self):
        """Convert standard representations of paths to a dropbox compatible format."""
        assert dropbox_cp._normalise_path(".") == ""
        assert dropbox_cp._normalise_path(r"Documents\Fred") == "/Documents/Fred"
        assert dropbox_cp._normalise_path("Documents/Fred") == "/Documents/Fred"
        assert dropbox_cp._normalise_path(Path(".")) == ""
        assert dropbox_cp._normalise_path(Path(r"Documents\Fred")) == "/Documents/Fred"
        assert dropbox_cp._normalise_path(Path("Documents/Fred")) == "/Documents/Fred"
