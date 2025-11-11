import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import helix
from helix.client import HelixNoValueFoundError

from cognee.infrastructure.databases.graph.graph_db_interface import (
    EdgeData,
    GraphDBInterface,
    Node,
    NodeData,
    record_graph_changes,
)
from cognee.infrastructure.engine import DataPoint
from cognee.modules.storage.utils import JSONEncoder
from cognee.shared.logging_utils import get_logger

logger = get_logger()


class HelixGraphAdapter(GraphDBInterface):
    """
    Adapter for HelixDB graph database operations with batch processing and async support.

    This class provides an interface for working with HelixDB, supporting efficient batch
    operations for nodes and edges, graph queries, and data retrieval. It implements the
    GraphDBInterface with optimized performance for large-scale graph operations.
    """

    def __init__(self, url: str, port: str, api_key: Optional[str] = None):
        self.url = url
        self.port = int(port)
        self.api_key = api_key
        self.executor = ThreadPoolExecutor()
        self.client: Optional[helix.Client] = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        try:
            is_local = not self.api_key or any(
                host in (self.url or "").lower() for host in ["127.0.0.1", "localhost"]
            )

            self.client = helix.Client(
                local=is_local,
                port=self.port,
                **({} if is_local else {"api_endpoint": self.url, "api_key": self.api_key}),
            )

            mode = "local" if is_local else "remote"
            logger.info(f"HelixDB client initialized: {mode} at {self.url}:{self.port}")
        except ImportError:
            raise ImportError("helix-py is not installed")
        except Exception as e:
            logger.error(f"HelixDB init failed: {e}")
            raise EnvironmentError(f"HelixDB connection error: {e}")

    async def query(self, query: str, params: Optional[dict] = None) -> Any:
        """
        Execute HelixDB query with consistent error handling and logging.
        Extract the 'data' if present otherwise return the raw result.

        Parameters:
        -----------

            - query (str): The HelixDB query url to be executed.
            - params (Optional[dict]): A dictionary of parameters for the query, if applicable.
              (default None)

        Returns:
        --------

            - Any: The result of the query execution, either a dictionary or a list.
        """
        if self.client is None:
            raise RuntimeError("HelixDB client is not initialized.")

        logger.debug(f"Executing query '{query}' with params {params}")
        try:
            loop = asyncio.get_running_loop()

            def blocking_call():
                return self.client.query(query, params or {})

            result = await loop.run_in_executor(self.executor, blocking_call)

            logger.debug(f"Executed query '{query}' successfully")
            logger.debug(f"Result: {result}")

            # First, unwrap single-element lists containing dicts
            if isinstance(result, list) and len(result) == 1 and isinstance(result[0], dict):
                result = result[0]

            return result
        except HelixNoValueFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to execute HelixDB query '{query}': {e}")
            raise

    async def is_empty(self) -> bool:
        """
        Check if the graph is empty.

        Returns:
        --------

            bool: True if the graph is empty, False otherwise.
        """
        try:
            result = await self.query("CogneeGetGraphData")
            if result and isinstance(result, dict):
                nodes_data = result.get("nodes", [])
                edges_data = result.get("edges", [])
                return len(nodes_data) == 0 and len(edges_data) == 0
            return False
        except Exception as e:
            logger.error(f"Failed to get graph data: {e}")
            raise

    @record_graph_changes
    async def add_node(self, node: DataPoint) -> None:
        """
        Add a single node to the graph if it doesn't exist.

        Parameters:
        -----------

            - node (DataPoint): The node to be added, represented as a DataPoint.
        """
        try:
            node_properties = node.model_dump() if hasattr(node, "model_dump") else vars(node)

            node_id = str(node_properties.get("id", ""))
            name = str(node_properties.get("name", node_id))
            node_type = str(node_properties.get("type", "Node"))

            other_properties = {
                k: v for k, v in node_properties.items() if k not in ["id", "name", "type"]
            }

            query_params = {
                "node_id": node_id,
                "name": name,
                "node_type": node_type,
                "properties": json.dumps(other_properties, cls=JSONEncoder),
            }

            result = await self.query("CogneeAddNode", query_params)
            logger.info(f"Node added successfully: {result}")

        except Exception as e:
            logger.error(f"Failed to add node: {e}")
            raise

    @record_graph_changes
    async def add_nodes(self, nodes: List[DataPoint]) -> None:
        """
        Add multiple nodes to the graph in a single operation.

        Parameters:
        -----------

            - nodes (Union[List[Node], List[DataPoint]]): A list of Node objects or DataPoint objects to be added to the graph.
        """
        if not nodes:
            return

        try:
            node_params = []
            for node in nodes:
                node_properties = node.model_dump() if hasattr(node, "model_dump") else vars(node)
                node_id = str(node_properties.get("id", ""))
                name = str(node_properties.get("name", node_id))
                node_type = str(node_properties.get("type", "Node"))
                other_properties = {
                    k: v for k, v in node_properties.items() if k not in ["id", "name", "type"]
                }
                node_params.append(
                    {
                        "node_id": node_id,
                        "name": name,
                        "node_type": node_type,
                        "properties": json.dumps(other_properties, cls=JSONEncoder),
                    }
                )

            if node_params:
                await self.query("CogneeAddNodes", {"nodes": node_params})
                logger.debug(f"Added {len(node_params)} nodes in batch")

        except Exception as e:
            logger.error(f"Failed to add nodes in batch: {e}")
            raise

    async def delete_node(self, node_id: str) -> None:
        """
        Delete a specified node from the graph by its ID.

        Parameters:
        -----------

            - node_id (str): Unique identifier for the node to delete.
        """
        try:
            query_params = {"node_id": node_id}
            result = await self.query("CogneeDeleteNode", query_params)
            logger.info(f"Node deleted successfully: {result}")
        except Exception as e:
            logger.error(f"Failed to delete node {node_id}: {e}")
            raise

    async def delete_nodes(self, node_ids: List[str]) -> None:
        """
        Delete multiple nodes from the graph by their identifiers.

        Parameters:
        -----------

            - node_ids (List[str]): A list of unique identifiers for the nodes to delete.
        """
        if not node_ids:
            return

        try:
            await self.query("CogneeDeleteNodes", {"node_ids": node_ids})
            logger.debug(f"Deleted {len(node_ids)} nodes in batch")
        except Exception as e:
            logger.error(f"Failed to delete nodes: {e}")
            raise

    async def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single node from the graph using its ID.

        Parameters:
        -----------

            - node_id (str): Unique identifier of the node to retrieve.

        Returns:
        --------

            Optional[Dict[str, Any]]: The node data if found, None otherwise.
        """
        try:
            query_params = {"node_id": node_id}
            result = await self.query("CogneeGetNode", query_params)

            # Handle both dict and list response formats
            if isinstance(result, list):
                if len(result) == 0 or result[0] is None:
                    return None
                result = result[0]

            if result and isinstance(result, dict):
                node_data = result.get("node")
                if not node_data:
                    return None

                node_data = self._parse_json_properties(node_data, "node", "id")
                return self._filter_node_fields(node_data)

            return None
        except Exception as e:
            logger.error(f"Failed to get node {node_id}: {e}")
            raise

    async def get_nodes(self, node_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieve multiple nodes from the graph using their IDs.

        Parameters:
        -----------

            - node_ids (List[str]): A list of unique identifiers for the nodes to retrieve.

        Returns:
        --------

            List[Dict[str, Any]]: List of node data for found nodes.
        """
        if not node_ids:
            return []

        try:
            result = await self.query("CogneeGetNodes", {"node_ids": node_ids})

            # Handle both dict and list response formats
            if isinstance(result, list):
                if len(result) == 0 or result[0] is None:
                    return []
                result = result[0]

            nodes = []
            if result and isinstance(result, dict):
                nodes_data = result.get("nodes", [])
                for node_data in nodes_data:
                    if not node_data:
                        continue
                    node_data = self._parse_json_properties(node_data, "node", "id")
                    nodes.append(self._filter_node_fields(node_data))

            return nodes
        except Exception as e:
            logger.error(f"Failed to get nodes: {e}")
            return []

    @record_graph_changes
    async def add_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create a new edge between two nodes in the graph.

        Parameters:
        -----------

            - source_id (str): The unique identifier of the source node.
            - target_id (str): The unique identifier of the target node.
            - relationship_name (str): The name of the relationship to be established by the edge.
            - properties (Optional[Dict[str, Any]]): Optional dictionary of properties associated with the edge. (default None)
        """
        try:
            edge_properties = properties or {}

            query_params = {
                "from_node_id": str(source_id),
                "to_node_id": str(target_id),
                "relationship_name": relationship_name,
                "properties": json.dumps(edge_properties, cls=JSONEncoder),
            }

            result = await self.query("CogneeAddEdge", query_params)
            logger.info(f"Edge added successfully: {result}")

        except Exception as e:
            logger.error(f"Failed to add edge: {e}")
            raise

    @record_graph_changes
    async def add_edges(
        self,
        edges: List[Tuple[str, str, str, Dict[str, Any]]],
    ) -> None:
        """
        Add multiple edges to the graph in a single operation.

        Parameters:
        -----------

            - edges (List[Tuple[str, str, str, Dict[str, Any]]]): A list of tuples representing edges to be added.
        """
        if not edges:
            return

        if self.client is None:
            raise RuntimeError("HelixDB client is not initialized.")

        try:
            edge_params = []
            for edge in edges:
                source_id, target_id, relationship_name, properties = edge
                edge_properties = properties

                edge_params.append(
                    {
                        "from_node_id": str(source_id),
                        "to_node_id": str(target_id),
                        "relationship_name": relationship_name,
                        "properties": json.dumps(edge_properties, cls=JSONEncoder),
                    }
                )

            if edge_params:
                await self.query("CogneeAddEdges", {"edges": edge_params})
                logger.debug(f"Added {len(edge_params)} edges in batch")

        except Exception as e:
            logger.error(f"Failed to add edges in batch: {e}")
            raise

    async def delete_graph(self) -> None:
        """
        Remove the entire graph, including all nodes and edges.
        """
        try:
            await self.query("CogneeDeleteGraph")
            logger.info("Graph deleted successfully")
        except Exception as e:
            logger.error(f"Failed to delete graph: {e}")
            raise

    async def get_graph_data(self) -> Tuple[List[Node], List[EdgeData]]:
        """
        Retrieve all nodes and edges within the graph.

        Returns:
        --------

            Tuple[List[Node], List[EdgeData]]: A tuple containing lists of all nodes and edges.
        """
        try:
            result = await self.query("CogneeGetGraphData")
            nodes = []
            edges = []
            if result and isinstance(result, dict):
                nodes_data = result.get("nodes", [])
                edges_data = result.get("edges", [])

                # Parse nodes
                for node_data in nodes_data:
                    if not node_data:
                        continue
                    node_id = node_data.get("id")
                    node_data = self._parse_json_properties(node_data, "node", "id")
                    node_props = self._filter_node_fields(node_data)
                    nodes.append((node_id, node_props))

                # Parse edges
                for edge_data in edges_data:
                    if not edge_data:
                        continue
                    edges.append(self._parse_edge_properties(edge_data, return_tuple=True))

            return nodes, edges
        except Exception as e:
            logger.error(f"Failed to get graph data: {e}")
            return [], []

    async def get_graph_metrics(self, include_optional: bool = False) -> Dict[str, Any]:
        raise NotImplementedError

    async def has_edge(self, source_id: str, target_id: str, relationship_name: str) -> bool:
        """
        Verify if an edge exists between two specified nodes.

        Parameters:
        -----------

            - source_id (str): Unique identifier of the source node.
            - target_id (str): Unique identifier of the target node.
            - relationship_name (str): Name of the relationship to verify.

        Returns:
        --------

            bool: True if the edge exists, False otherwise.
        """
        try:
            query_params = {
                "from_node_id": str(source_id),
                "to_node_id": str(target_id),
                "relationship_name": relationship_name,
            }
            result = await self.query("CogneeHasRelationship", query_params)
            if result and isinstance(result, dict):
                return bool(len(result.get("result", [])) > 0)
            return False
        except Exception as e:
            logger.error(f"Failed to check edge existence: {e}")
            return False

    async def has_edges(self, edges: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
        """
        Determine the existence of multiple edges in the graph.

        Parameters:
        -----------

            - edges (List[Tuple[str, str, str]]): A list of tuples representing edges to check for existence.

        Returns:
        --------

            List[Tuple[str, str, str]]: List of existing edges from the input list.
        """
        if not edges:
            return []

        check_tasks = [self.has_edge(str(edge[0]), str(edge[1]), edge[2]) for edge in edges]

        results = await asyncio.gather(*check_tasks, return_exceptions=True)

        # Filter edges based on results
        existing_edges = []
        for edge, result in zip(edges, results):
            if result is True and not isinstance(result, Exception):
                existing_edges.append(edge)

        return existing_edges

    async def get_edges(self, node_id: str) -> List[Tuple[Dict[str, Any], str, Dict[str, Any]]]:
        """
        Retrieve all edges that are connected to the specified node.

        Parameters:
        -----------

            - node_id (str): Unique identifier of the node whose edges are to be retrieved.

        Returns:
        --------

            List[Tuple[Dict[str, Any], str, Dict[str, Any]]]: List of tuples containing (source_node, relationship_name, target_node).
        """
        try:
            query_params = {"node_id": str(node_id)}
            result = await self.query("CogneeGetConnections", query_params)

            if result and isinstance(result, dict):
                return self._parse_connections_result(result, return_edge_props=False)
            return []
        except Exception as e:
            logger.error(f"Failed to get edges for node {node_id}: {e}")
            return []

    async def get_neighbors(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Get all neighboring nodes connected to the specified node.

        Parameters:
        -----------

            - node_id (str): Unique identifier of the node for which to retrieve neighbors.

        Returns:
        --------

            List[Dict[str, Any]]: List of neighboring node data.
        """
        try:
            query_params = {"node_id": str(node_id)}
            result = await self.query("CogneeGetNeighbors", query_params)

            neighbors = []
            if result and isinstance(result, dict):
                all_neighbors = result.get("incoming", []) + result.get("outgoing", [])
                for neighbor_data in all_neighbors:
                    if not neighbor_data:
                        continue
                    neighbor_data = self._parse_json_properties(neighbor_data, "neighbor", "id")
                    neighbors.append(self._filter_node_fields(neighbor_data))

            return neighbors
        except Exception as e:
            logger.error(f"Failed to get neighbors for node {node_id}: {e}")
            return []

    async def get_neighbours(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Get all neighbouring nodes connected to the specified node.

        This method provides British spelling compatibility for get_neighbors.

        Parameters:
        -----------

            - node_id (str): Unique identifier of the node for which to retrieve neighbours.

        Returns:
        --------

            List[Dict[str, Any]]: List of neighbouring node data.
        """
        return await self.get_neighbors(node_id)

    async def extract_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Extract a node by its ID.

        This method provides API compatibility with Kuzu adapter's extract_node method.

        Parameters:
        -----------

            - node_id (str): Unique identifier of the node to extract.

        Returns:
        --------

            Optional[Dict[str, Any]]: Node data as a dictionary if found, None otherwise.
        """
        node_data = await self.get_node(node_id)
        return dict(node_data) if node_data else None

    async def extract_nodes(self, node_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Extract multiple nodes by their IDs.

        This method provides API compatibility with Kuzu adapter's extract_nodes method.

        Parameters:
        -----------

            - node_ids (List[str]): List of unique identifiers for the nodes to extract.

        Returns:
        --------

            List[Dict[str, Any]]: List of node data dictionaries.
        """
        nodes_data = await self.get_nodes(node_ids)
        return [dict(node_data) for node_data in nodes_data]

    async def get_nodeset_subgraph(
        self, node_type: Type[Any], node_name: List[str]
    ) -> Tuple[List[Tuple[str, dict]], List[Tuple[str, str, str, dict]]]:
        """
        Fetch a subgraph consisting of a specific set of nodes and their relationships.

        Parameters:
        -----------

            - node_type (Type[Any]): The type of nodes to include in the subgraph.
            - node_name (List[str]): A list of names of the nodes to include in the subgraph.

        Returns:
        --------

            Tuple[List[Tuple[str, dict]], List[Tuple[str, str, str, dict]]]: Nodes and edges with string IDs.
        """
        if self.client is None:
            raise RuntimeError("HelixDB client is not initialized.")
        try:
            subgraph_tasks = [
                self._fetch_single_nodeset_subgraph(node_type.__name__, name) for name in node_name
            ]
            subgraph_results = await asyncio.gather(*subgraph_tasks, return_exceptions=True)

            all_nodes = {}
            all_edges = []

            for result in subgraph_results:
                if isinstance(result, Exception):
                    logger.warning(f"Failed to fetch subgraph: {result}")
                    continue
                if not isinstance(result, tuple) or len(result) != 2:
                    logger.warning(f"Invalid subgraph result format: {result}")
                    continue
                nodes, edges = result
                all_nodes.update(nodes)
                all_edges.extend(edges)

            unique_edges = self._deduplicate_edges(all_edges)

            # Format nodes as (node_id, node_data) tuples
            formatted_nodes = [(node_id, node_data) for node_id, node_data in all_nodes.items()]

            return formatted_nodes, unique_edges

        except Exception as e:
            logger.error(f"Failed to get nodeset subgraph: {e}")
            return [], []

    async def get_connections(
        self, node_id: str
    ) -> List[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
        """
        Get all nodes connected to a specified node and their relationship details.

        Parameters:
        -----------

            - node_id (Union[str, UUID]): Unique identifier of the node for which to retrieve connections.

        Returns:
        --------

            List[Tuple[NodeData, Dict[str, Any], NodeData]]: List of connection tuples (source, relationship, target).
        """
        if self.client is None:
            raise RuntimeError("HelixDB client is not initialized.")

        try:
            query_params = {"node_id": str(node_id)}
            result = await self.query("CogneeGetConnections", query_params)

            if result and isinstance(result, dict):
                return self._parse_connections_result(result, return_edge_props=True)
            return []
        except Exception as e:
            logger.error(f"Failed to get connections for node {node_id}: {e}")
            return []

    # ===== HELPER METHODS =====
    def _parse_json_properties(
        self, data: dict, data_type: str = "data", id_field: str = "id"
    ) -> dict:
        """
        Parse JSON properties from database result and merge into data dict.
        """
        if not data.get("properties"):
            return data

        try:
            props = json.loads(data["properties"])
            data.update(props)
            del data["properties"]
            return data
        except json.JSONDecodeError as e:
            item_id = data.get(id_field, "unknown")
            logger.warning(f"Failed to parse properties JSON for {data_type} {item_id}: {e}")
            return data

    def _filter_node_fields(self, node_data: dict) -> dict:
        """
        Filter out internal database fields and transform node data to expected format.
        Maps node_id to id if present, otherwise keeps id field.
        """

        # Fields to exclude from final output
        EXCLUDED_FIELDS = {"properties", "created_at", "updated_at", "label"}

        # Transform node_type to type for API consistency
        filtered_data = {}
        for k, v in node_data.items():
            if k not in EXCLUDED_FIELDS:
                if k == "node_type":
                    filtered_data["type"] = v
                elif k == "node_id":
                    # Map user-defined node_id to id for API consistency
                    filtered_data["id"] = v
                else:
                    filtered_data[k] = v

        return filtered_data

    def _parse_edge_properties(
        self, edge_raw: dict, return_tuple: bool = False
    ) -> Union[Dict[str, Any], EdgeData]:
        """
        Parse edge properties and optionally return full EdgeData tuple.
        """
        properties = {}
        if edge_raw.get("properties"):
            try:
                properties = json.loads(edge_raw["properties"])
            except json.JSONDecodeError as e:
                edge_id = edge_raw.get("id", "unknown")
                logger.warning(f"Failed to parse properties JSON for edge {edge_id}: {e}")

        if return_tuple:
            # Return full EdgeData tuple
            from_node = str(edge_raw.get("from_node", ""))
            to_node = str(edge_raw.get("to_node", ""))
            relationship_name = str(edge_raw.get("relationship_name", ""))
            return (from_node, to_node, relationship_name, properties)
        else:
            # Return just properties
            return properties

    async def _fetch_single_nodeset_subgraph(
        self, type_name: str, node_name: str
    ) -> Tuple[Dict[str, NodeData], List[EdgeData]]:
        """
        Fetch subgraph for a single node using HelixDB query.
        """

        query_params = {"node_type": type_name, "node_name": node_name}
        result = await self.query("CogneeGetNodesetSubgraph", query_params)

        if not result:
            return {}, []

        # Parse the result
        nodes = {}
        edges = []

        # Main node (comes as array with single element)
        if result.get("main_node") and len(result["main_node"]) > 0:
            main_node_raw = result["main_node"][0]
            main_node_raw = self._parse_json_properties(main_node_raw, "node", "id")
            nodes[main_node_raw["id"]] = self._filter_node_fields(main_node_raw)

        # In nodes
        for node_raw in result.get("in_nodes", []):
            node_raw = self._parse_json_properties(node_raw, "node", "id")
            nodes[node_raw["id"]] = self._filter_node_fields(node_raw)

        # Out nodes
        for node_raw in result.get("out_nodes", []):
            node_raw = self._parse_json_properties(node_raw, "node", "id")
            nodes[node_raw["id"]] = self._filter_node_fields(node_raw)

        # In edges
        for edge_raw in result.get("in_edges", []):
            edges.append(self._parse_edge_properties(edge_raw, return_tuple=True))

        # Out edges
        for edge_raw in result.get("out_edges", []):
            edges.append(self._parse_edge_properties(edge_raw, return_tuple=True))

        return nodes, edges

    def _parse_node_data(self, node_raw: dict) -> NodeData:
        """
        Parse raw node data from HelixDB into NodeData format.
        """

        node_data = dict(node_raw)
        node_data = self._parse_json_properties(node_data, "node", "id")
        return self._filter_node_fields(node_data)

    def _deduplicate_edges(self, edges: List[EdgeData]) -> List[EdgeData]:
        """
        Remove duplicate edges based on from_node, to_node, relationship_name, and properties.
        """
        seen_edges = set()
        unique_edges = []

        for edge in edges:
            src_id, tgt_id, rel_name, props = edge
            # Create a unique key for the edge
            edge_key = (
                src_id,
                tgt_id,
                rel_name,
                frozenset(props.items()) if props else frozenset(),
            )
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                unique_edges.append(edge)

        return unique_edges

    def _parse_connections_result(self, result: dict, return_edge_props: bool = True) -> List[Any]:
        """
        Parse CogneeGetConnections result into connection tuples.

        Parameters:
        -----------
            - result (dict): The query result from CogneeGetConnections
            - return_edge_props (bool): If True, return full edge properties dict.
                                      If False, return just relationship_name string.

        Returns:
        --------
            List of tuples in format:
            - (source_node, edge_properties, target_node) if return_edge_props=True
            - (source_node, relationship_name, target_node) if return_edge_props=False
        """
        connections = []

        main_node_raw = result.get("main_node", {})
        if not main_node_raw:
            return []

        main_node = self._parse_node_data(main_node_raw)

        # Create node lookup maps
        in_nodes = {node["id"]: self._parse_node_data(node) for node in result.get("in_nodes", [])}
        out_nodes = {
            node["id"]: self._parse_node_data(node) for node in result.get("out_nodes", [])
        }

        # Process outgoing connections (main_node -> target)
        for edge_raw in result.get("out_edges", []):
            target_id = edge_raw.get("to_node")
            if target_id in out_nodes:
                if return_edge_props:
                    properties = self._parse_edge_properties(edge_raw, return_tuple=False)
                    connections.append((main_node, properties, out_nodes[target_id]))
                else:
                    relationship_name = edge_raw.get("relationship_name", "")
                    connections.append(
                        (dict(main_node), relationship_name, dict(out_nodes[target_id]))
                    )

        # Process incoming connections (source -> main_node)
        for edge_raw in result.get("in_edges", []):
            source_id = edge_raw.get("from_node")
            if source_id in in_nodes:
                if return_edge_props:
                    properties = self._parse_edge_properties(edge_raw, return_tuple=False)
                    connections.append((in_nodes[source_id], properties, main_node))
                else:
                    relationship_name = edge_raw.get("relationship_name", "")
                    connections.append(
                        (dict(in_nodes[source_id]), relationship_name, dict(main_node))
                    )

        return connections
