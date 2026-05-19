# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from unittest.mock import Mock

import pytest
import requests
from pyhugegraph.utils.util import ResponseValidation


def test_response_validation_raises_http_error_with_numeric_status_body():
    response = Mock(spec=requests.Response)
    response.status_code = 400
    response.text = '{"status":400,"message":"bad gremlin"}'
    response.content = response.text.encode("utf-8")
    response.json.return_value = {"status": 400, "message": "bad gremlin"}
    response.request = Mock()
    response.request.body = "g.V2()"
    response.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Client Error")

    validator = ResponseValidation()

    with pytest.raises(Exception, match="bad gremlin"):
        validator(response, "POST", "/gremlin")
