import pytest

from hpcflow.archive.cloud.cloud import new_cloud_provider, DropboxCloudProvider
from hpcflow.config import Config


@pytest.fixture(scope="session")
def set_config():
    Config.set_config()


@pytest.mark.usefixtures("set_config")
class TestDropboxCloudProvider:

    def test_init_type(self):
        cloud_provider = new_cloud_provider("dropbox")
        assert isinstance(cloud_provider, DropboxCloudProvider)
