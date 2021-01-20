from unittest.mock import patch, Mock

import pytest
import requests

from hpcflow.archive.cloud.onedrive import OnedriveCloudProvider


@pytest.fixture(scope="session")
def cloud_provider() -> OnedriveCloudProvider:
    """Initialise a CloudProvider fixture to use for the tests. It doesn't matter that we
    initialise the Fixture with a fake token as authentication isn't done until needed
    and we patch all methods that might trigger authentication."""
    return OnedriveCloudProvider()


class TestOnedriveCloudProvider:
    @patch('hpcflow.archive.cloud.onedrive.requests.get')
    @patch('hpcflow.archive.cloud.onedrive.OnedriveCloudProvider._get_access_token')
    def test_check__good_access(self, mock_access_token, mock_requests_get, cloud_provider):
        mock_request_return = Mock(spec=requests.Response)
        mock_request_return.json = Mock(return_value={"displayName": "Bob"})
        mock_access_token.return_value = "aaa"
        mock_requests_get.return_value = mock_request_return
        assert cloud_provider.check_access() is True

    @patch('hpcflow.archive.cloud.onedrive.requests.get')
    @patch('hpcflow.archive.cloud.onedrive.OnedriveCloudProvider._get_access_token')
    def test_check_bad_access(self, mock_access_token, mock_requests_get, cloud_provider):
        mock_access_token.return_value = "aaa"
        mock_requests_get.side_effect = requests.exceptions.RequestException()
        assert cloud_provider.check_access() is False
