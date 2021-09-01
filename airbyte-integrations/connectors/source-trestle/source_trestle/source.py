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

from .auth import TrestleOAuth2Authenticator
from .streams import Properties
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

class SourceTrestle(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        authenticator = TrestleOAuth2Authenticator(client_id=config["client_id"], client_secret=config["client_secret"])

        try:
            token = authenticator.get_access_token()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = TrestleOAuth2Authenticator(client_id=config["client_id"], client_secret=config["client_secret"])
        args = {"authenticator": authenticator}
      
        return [
            Properties(**args)
        ]