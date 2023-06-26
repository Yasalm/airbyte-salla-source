#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams.http.requests_native_auth import SingleUseRefreshTokenOauth2Authenticator
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from typing import Dict
from datetime import datetime, timedelta, date


class SallaStream(HttpStream, ABC):
    """ """

    url_base = "https://api.salla.dev"
    primary_key = "id"
    count = 100

    def __init__(self, config: Dict, authenticator):
        super().__init__(authenticator=authenticator)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if "pagination" in response.json():
            response_json = response.json()["pagination"]
            if response_json["currentPage"] != response_json["totalPages"]:
                return {"page": response_json["currentPage"] + 1}
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """ """
        params = {"count": self.count}
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        if response_json.get("status") == 200:
            data = response_json.get("data", [])
            if data is not None:
                yield from data
            else:
                yield from []
        else:
            yield from []


class IncrementalSallaStream(SallaStream, ABC):
    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self._cursor_value = None
        self.defualt_filter_value = config[self.filter_field]

    @property
    @abstractmethod
    def cursor_field(self) -> str:
        """
        Defining a cursor field indicates that a stream is incremental, so any incremental stream must extend this class
        and define a cursor field.
        """
        pass

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest = datetime.strptime(latest_record[self.cursor_field][self.cursor_field], "%Y-%m-%d %H:%M:%S.%f")

        current = current_stream_state.get(self.cursor_field, None)
        if current == "" or current is None:
            current = self.defualt_filter_value
        if isinstance(current, str):
            current = datetime.strptime(current, "%d-%m-%Y")
        return {self.cursor_field: max(latest, current)}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        filter_value = stream_slice[self.cursor_field] if stream_slice else stream_state[self.cursor_field]
        params.update(
            {
                self.filter_field: filter_value,
            }
        )
        return params

    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        filter_value = (
            stream_state[self.cursor_field]
            if stream_state and self.cursor_field in stream_state
            else self.defualt_filter_value
        )
        yield from [{self.cursor_field: filter_value}]


class Orders(IncrementalSallaStream):
    cursor_field = "date"
    primary_key = "id"
    filter_field = "from_date"
    expanded = True  # equivalent to making '/order/{order_id}' api request. for returning order details.
    name = "orders"

    def path(self, **kwargs) -> str:
        path = "/admin/v2/orders"
        return path

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params.update(
            {
                "expanded": self.expanded,
            }
        )
        return params


class Customers(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/customers"


class Products(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/products"


class Categories(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/categories"


class Brands(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/brands"


class Branches(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/branches"


class SpecialOffers(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/specialoffers"


class CustomerGroups(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/customers/groups"


class AbandonedCarts(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/carts/abandoned"


class OrdersInvoice(IncrementalSallaStream):
    cursor_field = "date"
    filter_field = "from_date"
    name = "orders_invoice"

    def path(self, **kwargs):
        return "/admin/v2/orders/invoices"


class ProductTags(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/products/tags"


class ShippingCompanies(SallaStream):
    def path(self, **kwargs):
        return "/admin/v2/shipping/companies"


class SourceSalla(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        config["token_expiry_date"] = date.today() + timedelta(days=14)
        auth = SingleUseRefreshTokenOauth2Authenticator(
            connector_config=config,
            token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",
        )
        try:
            customersList_stream = Customers(config=config, authenticator=auth)
            customersList_records = customersList_stream.read_records(sync_mode="full_refresh")
            crecord = next(customersList_records)
            logger.info(f"succefulyy connected to Customers. Pulled on record {crecord}")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config["token_expiry_date"] = date.today() + timedelta(days=14)
        auth = SingleUseRefreshTokenOauth2Authenticator(
            connector_config=config,
            token_refresh_endpoint="https://accounts.salla.sa/oauth2/token",
        )
        if isinstance(config["from_date"], str):
            config["from_date"] = datetime.strptime(config["from_date"], "%Y-%m-%d")
        return [
            Orders(config=config, authenticator=auth),
            Customers(config=config, authenticator=auth),
            Products(config=config, authenticator=auth),
            Categories(config=config, authenticator=auth),
            Brands(config=config, authenticator=auth),
            Branches(config=config, authenticator=auth),
            SpecialOffers(config=config, authenticator=auth),
            CustomerGroups(config=config, authenticator=auth),
            AbandonedCarts(config=config, authenticator=auth),
            OrdersInvoice(config=config, authenticator=auth),
            ProductTags(config=config, authenticator=auth),
            ShippingCompanies(config=config, authenticator=auth),
        ]