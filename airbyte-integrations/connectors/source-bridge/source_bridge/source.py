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

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
import requests
import urllib.parse

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from datetime import datetime, timedelta

class BridgeStream(HttpStream):
    url_base = "https://api.bridgedataoutput.com/api/v2/OData/"
    
    primary_key = 'ListingKey'

    def __init__(self, dataset: str, brokerage_key: str, **kwargs):
        super().__init__(**kwargs)
        self.dataset = dataset
        self.brokerage_key = brokerage_key        

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f'{self.dataset}/Property'

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if response_json.get("@odata.nextLink"):
            next_query_string = urllib.parse.urlsplit(response_json.get("@odata.nextLink")).query
            params = dict(urllib.parse.parse_qsl(next_query_string))
            return {'$skip': params["$skip"]}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            '$select': 'ListAgentFullName,BridgeModificationTimestamp',
            '$filter': f'startswith(ListOfficeKey, \'{self.brokerage_key}\')',
            '$top': '200'
        }

        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        json_response = response.json()
        for record in json_response["value"]:
            yield record

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
            BridgeStream(**args)
        ]
