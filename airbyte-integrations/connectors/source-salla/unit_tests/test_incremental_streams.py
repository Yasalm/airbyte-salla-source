#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.models import SyncMode
from pytest import fixture
from source_salla.source import IncrementalSallaStream, Orders, OrdersInvoice
from datetime import datetime
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
import pytest


CLIENT_ID = "CLIENT_ID"
CLIENT_SECRET = "CLIENT_SECRET"
REFRESH_TOKEN = "REFRESH_TOKEN"

@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalSallaStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalSallaStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalSallaStream, "__abstractmethods__", set())


@fixture
def config():
    basic_config = {}
    basic_config["from_date"] = datetime.strptime("2022-12-31", "%Y-%m-%d")
    basic_config['credentials'] = {
        "CLIENT_ID": CLIENT_ID,
        "CLIENT_SECRET": CLIENT_SECRET,
        "REFRESH_TOKEN": REFRESH_TOKEN
    }
    return basic_config


def test_cursor_field(patch_incremental_base_class, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Orders(config=config, authenticator=auth)
    expected_cursor_field = "date"
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(patch_incremental_base_class, config):
    expected_state = {"date": datetime(2023, 3, 11, 14, 48, 20)}
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Orders(config=config, authenticator=auth)
    inputs = {"current_stream_state": {"date": ""}, "latest_record": {"date": {
        "date": "2023-03-11 14:48:20.000000",
        "timezone_type": 3,
        "timezone": "Asia/Riyadh"
      },}}
    assert stream.get_updated_state(**inputs) == expected_state


@pytest.mark.parametrize(
    "stream, kwargs, expected",
    [
        (
            Orders,
            {"sync_mode": SyncMode.incremental, "cursor_field": "date", "stream_state": {"date": "11-03-2023"}},
            {"date": "11-03-2023"},
        ),
        (
            OrdersInvoice,
            {"sync_mode": SyncMode.incremental, "cursor_field": "date", "stream_state": {"date": "11-03-2023"}},
            {"date": "11-03-2023"},
        ),
    ],
)
def test_stream_slices(patch_incremental_base_class, stream, kwargs, expected, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = stream(config=config, authenticator=auth)
    assert next(stream.stream_slices(**kwargs)) == expected


@pytest.mark.parametrize("stream", [(Orders), (OrdersInvoice)])
def test_supports_incremental(
    patch_incremental_base_class,
    stream,
    config
):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = stream(config=config, authenticator=auth)
    assert stream.supports_incremental

@pytest.mark.parametrize("stream", [(Orders), (OrdersInvoice)])
def test_source_defined_cursor(patch_incremental_base_class, stream, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = stream(config=config, authenticator=auth)
    assert stream.source_defined_cursor


@pytest.mark.parametrize(
    "stream, kwargs, expected",
    [
        (
            Orders,
            {"stream_state": {}, "stream_slice": {"date": "2022-12-12"}},
            {"count": 100, "from_date": "2022-12-12", "expanded": True},
        ),
        (
            OrdersInvoice,
            {"stream_state": {}, "stream_slice": {"date": "2022-12-12"}},
            {"count": 100, "from_date": "2022-12-12"},
        ),
    ],
)
def test_request_params(
    stream,
    kwargs,
    expected,
    config,
):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    assert stream(config=config, authenticator=auth).request_params(**kwargs) == expected
