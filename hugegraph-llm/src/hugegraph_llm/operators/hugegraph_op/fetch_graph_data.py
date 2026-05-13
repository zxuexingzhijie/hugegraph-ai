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


from typing import Any, Dict, Optional

from pyhugegraph.client import PyHugeClient


class FetchGraphData:
    def __init__(self, graph: PyHugeClient):
        self.graph = graph

    def run(self, graph_summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if graph_summary is None:
            graph_summary = {}

        # TODO: v_limit will influence the vid embedding logic in build_semantic_index.py
        v_limit = 10000
        e_limit = 200

        graph_api = self.graph.graph()

        vertices = graph_api.getVertexByCondition(limit=v_limit) or []
        edges = graph_api.getEdgeByPage(limit=e_limit)[0] or []

        vertex_ids = [str(v.id) for v in vertices]
        edge_ids = [str(e.id) for e in edges]

        graph_summary.update({
            "vertex_num": len(vertex_ids),
            "edge_num": len(edge_ids),
            "vertices": vertex_ids,
            "edges": edge_ids,
            "note": f"Only <={v_limit} VIDs and <={e_limit} EIDs for brief overview .",
        })
        return graph_summary
