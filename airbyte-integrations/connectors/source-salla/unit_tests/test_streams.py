#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock
from datetime import datetime

import pytest

from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
from source_salla.source import SallaStream, Orders, Customers, CustomerGroups, Products, Categories, Brands, Branches, SpecialOffers, AbandonedCarts, OrdersInvoice, ProductTags, ShippingCompanies



CLIENT_ID = "CLIENT_ID"
CLIENT_SECRET = "CLIENT_SECRET"
REFRESH_TOKEN = "REFRESH_TOKEN"


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(SallaStream, "path", "v0/example_endpoint")
    mocker.patch.object(SallaStream, "primary_key", "test_primary_key")
    mocker.patch.object(SallaStream, "__abstractmethods__", set())


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

def test_request_params(patch_base_class, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Customers(config=config, authenticator=auth)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": {"page": 2}}
    expected_params = {
        "count": 100,
        "page": 2
    }
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Orders(config=config,  authenticator=auth)
    response_data = {"data": [{"an_item": "a_value"}], "pagination": {
        "count": 15,
        "total": 30,
        "perPage": 15,
        "currentPage": 1,
        "totalPages": 2,
        "linkes": []
    }}
    inputs = {"response": MagicMock( json = lambda : response_data)}
    expected_token = {"page": 2}
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Customers(config=config, authenticator=auth)
    response_data = {
            "status": 200,
            "success": True,
            "data": [{"an_item": "a_value"}, {"an_item": "a_value"}], "pagination": {
        "count": 15,
        "total": 30,
        "perPage": 15,
        "currentPage": 1,
        "totalPages": 2,
        "linkes": []
    }}
    inputs = {"response": MagicMock( json = lambda: response_data)}
    expected_parsed_object = {"an_item": "a_value"}
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_http_method(patch_base_class, config):
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Customers(config=config, authenticator=auth)
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry, config):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Customers(config=config, authenticator=auth)
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class, config):
    response_mock = MagicMock()
    auth = Oauth2Authenticator(
        client_id=config['credentials'].get('client_id'), client_secret=config['credentials'].get('client_secret'), 
        refresh_token=config['credentials'].get('refresh_token'), token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",)
    stream = Customers(config=config, authenticator=auth)
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time