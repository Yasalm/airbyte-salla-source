#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock
from source_salla.source import SourceSalla
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
from datetime import datetime
import pytest

CLIENT_ID = "CLIENT_ID"
CLIENT_SECRET = "CLIENT_SECRET"
REFRESH_TOKEN = "REFRESH_TOKEN"

@pytest.fixture
def config():
    basic_config = {}
    basic_config["from_date"] = datetime.strptime("2022-12-31", "%Y-%m-%d")
    basic_config['credentials'] = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN
    }
    return basic_config

def test_check_connection(mocker, config):
    mocker.patch("source_salla.source.Customers.read_records", return_value=iter([{"id": 1}]))
    source = SourceSalla()
    logger_mock = MagicMock()
    assert source.check_connection(logger_mock, config) == (True, None)


def test_streams(mocker, config):
    source = SourceSalla()
    config_mock = {"from_date": "2022-12-31"}

    streams = source.streams(config,)
    expected_streams_number = 12
    assert len(streams) == expected_streams_number
