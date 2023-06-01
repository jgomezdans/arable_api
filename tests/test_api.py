import os
import requests
from unittest.mock import patch
import pytest

from src.arable_api import get_response

# Test cases


def test_get_response_without_api_key():
    # Test the case where no API key is provided and not defined in the
    # environment
    # Remove environment variables, as seen in
    # https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pytest/
    names_to_remove = {"ARABLE_API"}
    modified_environ = {
        k: v for k, v in os.environ.items() if k not in names_to_remove
    }
    with patch.dict(os.environ, modified_environ, clear=True):
        with pytest.raises(KeyError):
            get_response("devices")


def test_get_response_with_invalid_api_key():
    # Test the case where an invalid API key is provided
    api_key = "invalid_key"
    with pytest.raises(requests.HTTPError):
        get_response("service", api_key=api_key)


def test_get_response_success():
    # Test a successful API call with valid parameters
    service = "example_service"
    parameters = {"param1": "value1", "param2": "value2"}
    expected_result = {"key": "value"}

    # Mock the requests library to return a successful response
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = '{"key": "value"}'

        result = get_response(service, parameters)

        mock_get.assert_called_once_with(
            f"https://api.arable.cloud/api/v2/{service}",
            headers={"Authorization": "apikey ARABLE_API_KEY"},
            params=parameters,
        )
        assert result == expected_result


def test_get_response_http_error():
    # Test a failed API call with non-200 status code
    service = "example_service"
    parameters = {"param1": "value1"}

    # Mock the requests library to return a non-200 status code
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        mock_get.return_value.content = "Not found"

        with pytest.raises(requests.HTTPError):
            get_response(service, parameters)

        mock_get.assert_called_once_with(
            f"https://api.arable.cloud/api/v2/{service}",
            headers={"Authorization": "apikey ARABLE_API_KEY"},
            params=parameters,
        )


# Environment setup


@pytest.fixture(autouse=True)
def mock_api_key(monkeypatch: pytest.MonkeyPatch):
    # Mock the environment variable for the API key
    monkeypatch.setenv("ARABLE_API", "ARABLE_API_KEY")
