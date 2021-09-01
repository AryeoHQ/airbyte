#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import pendulum
import requests
import urllib.parse

from abc import ABC
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from datetime import date, datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

class BridgeStream(HttpStream, ABC):
    """
    Core stream class. Captures config values needed for all streams.

    This class is not intended to be used directly, subclass it instead.
    """
    url_base = 'https://api.bridgedataoutput.com/api/v2/OData/'
    datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, dataset: str, brokerage_key: str, **kwargs):
        super(BridgeStream, self).__init__(**kwargs)
        self.dataset = dataset
        self.brokerage_key = brokerage_key

class PaginatedBridgeStream(BridgeStream):
    """
    A paginated stream.

    This class is not intended to be used directly, subclass it instead.
    """
    data_key = 'value'
    next_page_key = '@odata.nextLink'
    skip_key = '$skip'

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if response_json.get(self.next_page_key):
            next_query_string = urllib.parse.urlsplit(response_json.get(self.next_page_key)).query
            params = dict(urllib.parse.parse_qsl(next_query_string))
            return {self.skip_key: params[self.skip_key]}

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        if next_page_token:
            params = {}
            params.update(next_page_token)
            return params
        else:
            return {}

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None) -> Iterable[Mapping]:
        json_response = response.json()
        for record in json_response[self.data_key]:
            yield record

class IncrementalBridgeStream(PaginatedBridgeStream):
    """
    A paginated stream that can detect new or modified data using
    a primary key and cursor (updated_at) field.

    This class is not intended to be used directly, subclass it instead.
    """
    primary_key = None
    cursor_field = None

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None) -> Iterable[Mapping]:
        json_response = response.json()
        for record in json_response[self.data_key]:
            if stream_state:
                if record[self.cursor_field] >= stream_state.get(self.cursor_field):
                    yield record
            else:
                yield record

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        """
        This method is called once for each record returned from the API.
        Compare each record's updated timestamp to the state's last synced timestamp.
        If this is the first time we run a sync or no state was passed, current_stream_state will be None.
        """
        if current_stream_state is not None and self.cursor_field in current_stream_state:
            current_parsed_date = datetime.strptime(current_stream_state[self.cursor_field], self.datetime_format)
            latest_record_date = datetime.strptime(latest_record[self.cursor_field], self.datetime_format)
            return {self.cursor_field: max(current_parsed_date, latest_record_date).strftime(self.datetime_format)}
        else:
            return {self.cursor_field: datetime.today().strftime(self.datetime_format)}

class Properties(IncrementalBridgeStream):
    """
    An incremental, paginated stream that captures real-estate properties from the Bridge API.
    """
    primary_key = 'ListingKey'
    cursor_field = 'ModificationTimestamp'
    page_size = '200'

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return f'{self.dataset}/Property'

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        config_fields = 'ListAgentFullName'
        select_fields = f'ModificationTimestamp,{config_fields}'

        params = {
            '$select': select_fields,
            '$filter': f'startswith(ListOfficeKey, \'{self.brokerage_key}\')',
            '$top': self.page_size
        }

        if next_page_token:
            params.update(next_page_token)

        return params

class SourceBridge(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        if config["server_token"].isalnum():
            authenticator = TokenAuthenticator(config["server_token"])
            return True, None
        else:
            return False, f"Server token should be alphanumeric."

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = TokenAuthenticator(config["server_token"])
        args = {"authenticator": authenticator, "dataset": config["dataset"], "brokerage_key": config["brokerage_key"]}

        return [
            Properties(**args)
        ]
