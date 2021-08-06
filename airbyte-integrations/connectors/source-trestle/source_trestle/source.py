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
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from datetime import datetime, timedelta

class TrestleOauth2Authenticator(HttpAuthenticator):
    def __init__(self, client_id: str, client_secret: str, scopes: List[str] = None):
        self.token_refresh_endpoint = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
        self.client_secret = client_secret
        self.client_id = client_id        
        self.scopes = ["api"]

        self._token_expiry_date = pendulum.now().subtract(hours=8)
        self._access_token = None

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    def get_access_token(self):
        if self.token_has_expired():
            t0 = pendulum.now()
            token, expires_in = self.refresh_access_token()
            self._access_token = token
            self._token_expiry_date = t0.add(seconds=expires_in)

        return self._access_token

    def token_has_expired(self) -> bool:
        return pendulum.now() > self._token_expiry_date

    def get_refresh_request_body(self) -> Mapping[str, Any]:        
        payload: MutableMapping[str, Any] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,            
        }

        if self.scopes:
            payload["scopes"] = self.scopes

        return payload

    def refresh_access_token(self) -> Tuple[str, int]:
        try:            
            response = requests.request(method="POST", url=self.token_refresh_endpoint, data=self.get_refresh_request_body())
            response.raise_for_status()
            response_json = response.json()
            return response_json["access_token"], response_json["expires_in"]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e

class TrestleStream(HttpStream):
    url_base = "https://api-prod.corelogic.com/trestle/"
    
    primary_key = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "odata/Property"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            '$select': 'ListAgentFullName,ListAgentFirstName',
            '$top': '1000'
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
        return [response.json()]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if response_json.get("@odata.nextLink"):
            next_query_string = urllib.parse.urlsplit(response_json.get("@odata.nextLink")).query
            params = dict(urllib.parse.parse_qsl(next_query_string))
            return {'$skip': params["$skip"]}

class SourceTrestle(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        auth = TrestleOauth2Authenticator(            
            client_id=config["client_id"],
            client_secret=config["client_secret"]
            )

        try:
            token = auth.get_access_token()      
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TrestleOauth2Authenticator(            
            client_id=config["client_id"],
            client_secret=config["client_secret"]
            )
        args = {"authenticator": auth}
      
        return [TrestleStream(**args)]
