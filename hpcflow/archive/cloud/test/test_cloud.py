"""Tests for methods in the cloud.py file"""

from pathlib import Path

import pytest

import hpcflow.archive.cloud.cloud as cloud
from hpcflow.archive.errors import ArchiveError


class TestCloud:
    def test_read_file_contents(self):
        sample_path = Path(__file__).parent / "sample_file.txt"
        file_contents = cloud.read_file_contents(sample_path)
        assert isinstance(file_contents, bytes) is True
        assert file_contents == b"Sample file used for testing."

    def test_read_missing_file(self):
        sample_path = Path("non-existent-path")
        with pytest.raises(ArchiveError):
            cloud.read_file_contents(sample_path)

    def test_exclude_specified_files(self):
        exclusion_list = ["*.txt", "b.png"]
        file_list = ["a.txt", "b.txt", "a.png", "b.png"]
        expected = ["a.png"]
        assert cloud.exclude_specified_files(file_list, exclusion_list) == expected
