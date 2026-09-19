"""
test_ingestion.py — Unit Tests for Data Ingestion Module
Project: COVID-19 Big Data Analytics Platform
Author:  Sarth Narola (Member 2 — Cluster & Data Ingestion Engineer)

Run:  pytest tests/test_ingestion.py -v
"""

import pytest
import responses
import requests
from pathlib import Path
from unittest.mock import patch, MagicMock
import tempfile
import os


class TestDataDownloader:
    """Tests for the data_downloader module."""

    def test_covid_data_files_list_not_empty(self):
        """The list of files to download should not be empty."""
        from src.ingestion.data_downloader import COVID_DATA_FILES
        assert len(COVID_DATA_FILES) > 0

    def test_covid_data_files_are_csv(self):
        """All configured download files should be .csv format."""
        from src.ingestion.data_downloader import COVID_DATA_FILES
        for f in COVID_DATA_FILES:
            assert f.endswith(".csv"), f"{f} is not a CSV file"

    def test_base_url_is_https(self):
        """Base URL should use HTTPS for secure download."""
        from src.ingestion.data_downloader import BASE_URL
        assert BASE_URL.startswith("https://"), "BASE_URL must use HTTPS"

    @patch("src.ingestion.data_downloader.requests.get")
    def test_download_file_success(self, mock_get, tmp_path):
        """download_file should write content to disk on success."""
        from src.ingestion.data_downloader import download_file

        # Mock a successful HTTP response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-length": "100"}
        mock_response.iter_content = lambda chunk_size: [b"date,location\n", b"2021,IN\n"]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        dest = tmp_path / "test.csv"
        result = download_file("https://fake-url/test.csv", dest)

        assert result is True
        assert dest.exists()
        assert dest.stat().st_size > 0

    @patch("src.ingestion.data_downloader.requests.get")
    def test_download_file_http_error(self, mock_get, tmp_path):
        """download_file should return False on HTTP error."""
        from src.ingestion.data_downloader import download_file

        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

        dest = tmp_path / "fail.csv"
        result = download_file("https://bad-url/fail.csv", dest)
        assert result is False

    def test_output_dir_created_if_missing(self, tmp_path):
        """download_all should create output_dir if it doesn't exist."""
        from src.ingestion.data_downloader import download_all

        new_dir = tmp_path / "new_output"
        assert not new_dir.exists()

        with patch("src.ingestion.data_downloader.download_file", return_value=True):
            download_all(output_dir=new_dir)

        assert new_dir.exists()


class TestHdfsUploader:
    """Tests for the hdfs_uploader module."""

    def test_hdfs_raw_path_is_correct(self):
        """HDFS raw path should match project config."""
        from src.ingestion.hdfs_uploader import HDFS_RAW_PATH
        assert HDFS_RAW_PATH == "/covid/raw"

    def test_upload_all_returns_empty_on_no_csv(self, tmp_path):
        """upload_all should return empty dict if no CSV files present."""
        from src.ingestion.hdfs_uploader import upload_all
        result = upload_all(dataset_dir=tmp_path)
        assert result == {}

    @patch("src.ingestion.hdfs_uploader.subprocess.run")
    def test_upload_file_calls_docker_cp(self, mock_run, tmp_path):
        """upload_file should call docker cp to copy file into container."""
        from src.ingestion.hdfs_uploader import upload_file

        # Create a fake CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("date,location\n2021-01-01,IN\n")

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = ""
        mock_proc.stderr = ""
        mock_run.return_value = mock_proc

        result = upload_file(csv_file, "/covid/raw")
        assert result is True
        # docker cp should have been called
        calls = [str(c) for c in mock_run.call_args_list]
        assert any("docker" in c and "cp" in c for c in calls)
