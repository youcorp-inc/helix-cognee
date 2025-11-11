"""Unit tests for HelixDB Graph Database Adapter."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from cognee.infrastructure.databases.graph.helixdb.adapter import HelixGraphAdapter


@pytest.fixture
def adapter():
    """Create adapter with mocked client."""
    with patch(
        "cognee.infrastructure.databases.graph.helixdb.adapter.HelixGraphAdapter._initialize_client"
    ):
        a = HelixGraphAdapter(url="http://localhost:6969", port="6969", api_key="test_key")
        a.client = MagicMock()
        return a


@pytest.fixture
def node_data():
    """Standard node response data."""
    return {
        "node": {
            "id": "internal_1",
            "node_id": "node1",
            "name": "TestNode",
            "node_type": "Entity",
            "properties": '{"key": "value"}',
            "created_at": "2025-10-28T06:51:30.000000",
            "updated_at": "2025-10-28T06:51:30.000000",
        }
    }


def mock_node(node_id="node1", name="Test", **props):
    """Create mock node object."""
    m = MagicMock()
    m.id = m.node_id = node_id
    m.name = name
    m.type = "Entity"
    m.model_dump.return_value = {
        "id": node_id,
        "node_id": node_id,
        "name": name,
        "type": "Entity",
        **props,
    }
    return m


class TestNodes:
    """Node operation tests."""

    @pytest.mark.asyncio
    async def test_add_node(self, adapter, node_data):
        adapter.client.query.return_value = node_data
        await adapter.add_node(mock_node(str(uuid4()), "Test", extra="prop"))

        call = adapter.client.query.call_args[0]
        assert call[0] == "CogneeAddNode"
        assert call[1]["name"] == "Test"
        assert "extra" in call[1]["properties"]

    @pytest.mark.asyncio
    async def test_add_nodes(self, adapter):
        adapter.client.query.return_value = None
        nodes = [mock_node(str(uuid4()), "Node1"), mock_node(str(uuid4()), "Node2")]
        await adapter.add_nodes(nodes)

        params = adapter.client.query.call_args[0][1]
        assert len(params["nodes"]) == 2
        assert params["nodes"][0]["name"] == "Node1"

    @pytest.mark.asyncio
    async def test_get_node(self, adapter, node_data):
        adapter.client.query.return_value = node_data
        result = await adapter.get_node("node1")

        assert result == {
            "id": "node1",
            "name": "TestNode",
            "type": "Entity",
            "key": "value",
        }
        adapter.client.query.assert_called_once_with("CogneeGetNode", {"node_id": "node1"})

    @pytest.mark.asyncio
    async def test_get_nodes(self, adapter):
        adapter.client.query.return_value = {
            "nodes": [
                {
                    "id": "n1",
                    "name": "N1",
                    "node_type": "Entity",
                    "properties": '{"k": "v1"}',
                },
                {
                    "id": "n2",
                    "name": "N2",
                    "node_type": "Entity",
                    "properties": '{"k": "v2"}',
                },
            ]
        }
        result = await adapter.get_nodes(["n1", "n2"])

        assert len(result) == 2
        assert result[0]["id"] == "n1"
        assert result[1]["k"] == "v2"

    @pytest.mark.asyncio
    async def test_delete_node(self, adapter):
        adapter.client.query.return_value = None
        await adapter.delete_node("n1")
        adapter.client.query.assert_called_once_with("CogneeDeleteNode", {"node_id": "n1"})

    @pytest.mark.asyncio
    async def test_delete_nodes(self, adapter):
        adapter.client.query.return_value = None
        await adapter.delete_nodes(["n1", "n2"])

        call = adapter.client.query.call_args[0]
        assert call[0] == "CogneeDeleteNodes"
        assert call[1]["node_ids"] == ["n1", "n2"]


class TestEdges:
    """Edge operation tests."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("props,expected", [({"weight": 1}, '{"weight": 1}'), (None, "{}")])
    async def test_add_edge(self, adapter, props, expected):
        adapter.client.query.return_value = {"edge": {}}
        await adapter.add_edge("n1", "n2", "REL", props)

        params = adapter.client.query.call_args[0][1]
        assert params["from_node_id"] == "n1"
        assert params["relationship_name"] == "REL"
        assert params["properties"] == expected

    @pytest.mark.asyncio
    async def test_add_edges(self, adapter):
        adapter.client.query.return_value = None
        id1, id2, id3 = str(uuid4()), str(uuid4()), str(uuid4())
        edges = [(id1, id2, "REL1", {"p": "v"}), (id2, id3, "REL2", {})]
        await adapter.add_edges(edges)

        params = adapter.client.query.call_args[0][1]
        assert len(params["edges"]) == 2
        assert params["edges"][0]["relationship_name"] == "REL1"

    @pytest.mark.asyncio
    async def test_get_edges(self, adapter):
        adapter.client.query.return_value = {
            "main_node": {
                "id": "n1",
                "name": "N1",
                "node_type": "Entity",
                "properties": "{}",
            },
            "out_nodes": [
                {
                    "id": "n2",
                    "name": "N2",
                    "node_type": "Entity",
                    "properties": "{}",
                }
            ],
            "in_nodes": [],
            "in_edges": [],
            "out_edges": [
                {
                    "from_node": "n1",
                    "to_node": "n2",
                    "relationship_name": "REL",
                    "properties": "{}",
                }
            ],
        }
        result = await adapter.get_edges("n1")

        assert len(result) == 1
        assert result[0][1] == "REL"

    @pytest.mark.asyncio
    async def test_has_edge(self, adapter):
        adapter.client.query.return_value = {"result": [{"id": "e1"}]}
        result = await adapter.has_edge("n1", "n2", "REL")

        assert result is True
        params = adapter.client.query.call_args[0][1]
        assert params["relationship_name"] == "REL"

    @pytest.mark.asyncio
    async def test_has_edge_false(self, adapter):
        adapter.client.query.return_value = {"result": []}
        assert await adapter.has_edge("n1", "n2", "REL") is False


class TestGraph:
    """Graph operation tests."""

    @pytest.mark.asyncio
    async def test_delete_graph(self, adapter):
        adapter.client.query.return_value = None
        await adapter.delete_graph()
        adapter.client.query.assert_called_once_with("CogneeDeleteGraph", {})

    @pytest.mark.asyncio
    async def test_get_graph_data(self, adapter):
        adapter.client.query.return_value = {
            "nodes": [
                {
                    "id": "n1",
                    "name": "N1",
                    "node_type": "Entity",
                    "properties": "{}",
                }
            ],
            "edges": [
                {
                    "from_node": "n1",
                    "to_node": "n2",
                    "relationship_name": "REL",
                    "properties": "{}",
                }
            ],
        }
        nodes, edges = await adapter.get_graph_data()

        assert len(nodes) == 1
        assert len(edges) == 1
        assert edges[0][2] == "REL"

    @pytest.mark.asyncio
    async def test_get_neighbors(self, adapter):
        adapter.client.query.return_value = {
            "incoming": [
                {
                    "id": "n1",
                    "name": "N1",
                    "node_type": "Entity",
                    "properties": "{}",
                }
            ],
            "outgoing": [
                {
                    "id": "n2",
                    "name": "N2",
                    "node_type": "Entity",
                    "properties": "{}",
                }
            ],
        }
        result = await adapter.get_neighbors("node_id")

        assert len(result) == 2
        assert result[0]["id"] == "n1"
        assert result[1]["id"] == "n2"

    @pytest.mark.asyncio
    async def test_get_connections(self, adapter):
        adapter.client.query.return_value = {
            "main_node": {
                "id": "main",
                "name": "Main",
                "node_type": "test",
                "properties": "{}",
            },
            "in_nodes": [
                {
                    "id": "src",
                    "name": "Src",
                    "node_type": "test",
                    "properties": "{}",
                }
            ],
            "out_nodes": [
                {
                    "id": "tgt",
                    "name": "Tgt",
                    "node_type": "test",
                    "properties": "{}",
                }
            ],
            "in_edges": [
                {
                    "from_node": "src",
                    "to_node": "main",
                    "relationship_name": "IN",
                    "properties": "{}",
                }
            ],
            "out_edges": [
                {
                    "from_node": "main",
                    "to_node": "tgt",
                    "relationship_name": "OUT",
                    "properties": "{}",
                }
            ],
        }
        result = await adapter.get_connections("main")

        assert len(result) == 2
        assert any(c[0]["id"] == "src" for c in result)
        assert any(c[2]["id"] == "tgt" for c in result)

    @pytest.mark.asyncio
    async def test_get_nodeset_subgraph(self, adapter):
        adapter.client.query.return_value = {
            "main_node": [
                {
                    "id": "n1",
                    "name": "Test Node",
                    "node_type": "TestType",
                    "properties": '{"extra": "val1"}',
                }
            ],
            "in_nodes": [],
            "out_nodes": [
                {
                    "id": "n2",
                    "name": "Neighbor",
                    "node_type": "TestType",
                    "properties": '{"extra": "val2"}',
                }
            ],
            "in_edges": [],
            "out_edges": [
                {
                    "id": "e1",
                    "from_node": "n1",
                    "to_node": "n2",
                    "relationship_name": "connects_to",
                    "properties": '{"weight": 1}',
                }
            ],
        }

        from cognee.infrastructure.engine import DataPoint

        class TestType(DataPoint):
            pass

        nodes, edges = await adapter.get_nodeset_subgraph(TestType, ["Test Node"])

        assert len(nodes) == 2
        assert nodes[0] == (
            "n1",
            {"id": "n1", "name": "Test Node", "type": "TestType", "extra": "val1"},
        )
        assert nodes[1] == (
            "n2",
            {"id": "n2", "name": "Neighbor", "type": "TestType", "extra": "val2"},
        )
        assert len(edges) == 1
        assert edges[0] == ("n1", "n2", "connects_to", {"weight": 1})
