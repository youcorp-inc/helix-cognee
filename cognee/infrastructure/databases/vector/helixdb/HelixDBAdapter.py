import asyncio
import asyncio
import json
import helix
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urlunparse
from typing import List, Optional, cast, Dict, Any
from uuid import UUID

from helix.client import HelixNoValueFoundError

from cognee.infrastructure.databases.vector.models.ScoredResult import ScoredResult
from cognee.infrastructure.databases.vector.vector_db_interface import VectorDBInterface
from cognee.infrastructure.databases.exceptions import MissingQueryParameterError
from cognee.infrastructure.engine import DataPoint
from cognee.infrastructure.engine.utils import parse_id
from cognee.modules.storage.utils import get_own_properties, JSONEncoder
from cognee.shared.logging_utils import get_logger

from ..embeddings.EmbeddingEngine import EmbeddingEngine

logger = get_logger("HelixDBAdapter")


class HelixDBAdapter(VectorDBInterface):
    name = "HelixDB"
    url: Optional[str]
    api_key: Optional[str]
    connection: Optional[helix.Client] = None

    def __init__(
        self, url: Optional[str], api_key: Optional[str], embedding_engine: EmbeddingEngine
    ):
        self.embedding_engine = embedding_engine
        self.url = url
        self.api_key = api_key
        self.executor = ThreadPoolExecutor()
        self._initialize_connection()

    def _initialize_connection(self) -> None:
        try:
            parsed = urlparse(self.url or "http://localhost:6969")
            port = parsed.port
            url_lower = (self.url or "http://localhost:6969").lower()
            # Check if URL contains host.docker.internal - if so, always treat as remote
            is_docker_host = "host.docker.internal" in url_lower
            
            # Determine if connection is local:
            # - If host.docker.internal, always remote (even without api_key)
            # - Otherwise, local only if no api_key AND URL contains localhost/127.0.0.1
            if is_docker_host:
                is_local = False
            else:
                is_local = not self.api_key and any(
                    host in url_lower for host in ["127.0.0.1", "localhost"]
                )

            # When remote, construct clean base URL (scheme + netloc) without path
            # The helix client will append paths/queries, so we need a clean base URL
            if not is_local:
                # Ensure port is in netloc if not already there
                if not port:
                    port = 6969
                netloc = f"{parsed.hostname}:{port}" if parsed.hostname else f":{port}"
                api_endpoint = urlunparse((parsed.scheme, netloc, "", "", "", ""))
            else:
                api_endpoint = None

            self.connection = helix.Client(
                local=is_local,
                port=port if port else 6969,
                **({} if is_local else {"api_endpoint": api_endpoint, "api_key": self.api_key}),
            )

            mode = "local" if is_local else "remote"
            logger.info(f"HelixDB client initialized: {mode} at {self.url}")
        except ImportError:
            raise ImportError("helix-py is not installed")
        except Exception as e:
            logger.error(f"HelixDB init failed: {e}")
            raise EnvironmentError(f"HelixDB connection error: {e}")

    async def _query(self, query_name: str, params: dict):
        """
        Execute a HelixDB queries.
        """
        if self.connection is None:
            raise RuntimeError("HelixDB client is not initialized.")
        connection = self.connection

        logger.debug(f"Executing HelixDB query '{query_name}'")
        try:
            loop = asyncio.get_running_loop()

            def blocking_call():
                return connection.query(query_name, params)

            result = await loop.run_in_executor(self.executor, blocking_call)
            logger.debug(f"Executed HelixDB query '{query_name}' successfully")

            # unwrap single-element lists containing dicts
            if isinstance(result, list) and len(result) == 1 and isinstance(result[0], dict):
                result = result[0]

            return result
        except HelixNoValueFoundError:
            logger.debug(f"HelixDB query '{query_name}' returned no value")
            return None
        except Exception as e:
            logger.error(f"Failed to execute HelixDB query '{query_name}': {e}")
            raise

    async def embed_data(self, data: list[str]) -> list[list[float]]:
        """
        Embed textual data into vector representations.

        Parameters:
        -----------
        - data (list[str]): List of strings to embed.
        """
        return await self.embedding_engine.embed_text(data)

    async def has_collection(self, collection_name: str) -> bool:
        """
        Check if a collection exists in HelixDB.

        Parameters:
        -----------
        - collection_name (str): Name of the collection to check.

        Returns:
        --------
        - bool: True if collection exists and has data points, False otherwise.
        """
        try:
            result = await self._get_collection_by_name(collection_name)
            if result is None:
                return False
            return len(result) > 0
        except Exception as e:
            logger.error(f"Failed to check collection {collection_name}: {e}")
            return False

    async def create_collection(self, collection_name: str, payload_schema=None):
        """
        In HelixDB, collections are created implicitly when data is added,
        so this is a no-op placeholder for interface compatibility.
        """
        # HelixDB creates collections implicitly when first data point is added
        # No explicit collection creation needed
        logger.debug(f"Collection {collection_name} will be created implicitly")
        pass

    async def create_data_points(self, collection_name: str, data_points: list[DataPoint]):
        """
        Create and upsert data points into a collection.

        Parameters:
        -----------
        - collection_name (str): Name of the collection.
        - data_points (list[DataPoint]): List of DataPoint instances to add.
        """
        try:
            contents = [DataPoint.get_embeddable_data(data_point) for data_point in data_points]
            vectors = await self.embed_data(cast(list[str], contents))

            return await asyncio.gather(
                *(
                    self._create_data_point(
                        collection_name,
                        vectors[i],
                        str(dp.id),
                        json.dumps(get_own_properties(dp), cls=JSONEncoder),
                        cast(str, contents[i]),
                    )
                    for i, dp in enumerate(data_points)
                )
            )
        except Exception as e:
            logger.error(f"Failed to create data points in {collection_name}: {e}")
            raise

    async def retrieve(self, collection_name: str, data_point_ids: list[str]):
        """
        Retrieve data points from a collection by IDs.

        Parameters:
        -----------
        - collection_name (str): Name of the collection.
        - data_point_ids (list[str]): List of data point IDs to retrieve.

        Returns:
        --------
        - List[ScoredResult]: List of ScoredResult instances for each data point.
        """
        try:
            result = await self._query(
                "CogneeRetrieve", {"collection_name": collection_name, "dp_ids": data_point_ids}
            )

            scored_results = []
            result_dict = cast(Dict[str, Any], result)
            if isinstance(result_dict.get("documents"), list):
                docs = cast(List[Dict[str, Any]], result_dict["documents"])
                for doc in docs:
                    scored_results.append(self._parse_doc_to_scored_result(doc, score=0))
            return scored_results
        except Exception as e:
            logger.error(f"Failed to retrieve data points from {collection_name}: {e}")
            return []

    async def search(
        self,
        collection_name: str,
        query_text: Optional[str] = None,
        query_vector: Optional[List[float]] = None,
        limit: Optional[int] = 15,
        with_vector: bool = False,
    ):
        """
        Search for data points in a collection using either a text or a vector query.

        Parameters:
        -----------
        - collection_name (str): Name of the collection.
        - query_text (Optional[str]): Text query to search for.
        - query_vector (Optional[List[float]]): Vector query to search for.
        - limit (Optional[int]): Maximum number of results to return.

        Returns:
        --------
        - List[ScoredResult]: List of ScoredResult instances representing the search results.
        """
        if query_text is None and query_vector is None:
            raise MissingQueryParameterError()

        if query_text and not query_vector:
            query_vector = (await self.embed_data([query_text]))[0]

        if limit is None:
            try:
                result = await self._get_collection_by_name(collection_name)
                if result is None:
                    return []
                limit = len(result)
            except Exception as e:
                logger.error(f"Failed to get collection {collection_name}: {e}")
                raise

        try:
            result = await self._query(
                "CogneeSearch",
                {"collection_name": collection_name, "vector": query_vector, "limit": limit},
            )

            scored_results = []
            result_dict = cast(Dict[str, Any], result)

            if isinstance(result_dict.get("result"), list):
                docs = cast(List[Dict[str, Any]], result_dict["result"])
                for doc in docs:
                    score = float(doc.get("score", 0))
                    scored_results.append(self._parse_doc_to_scored_result(doc, score=score))

            return scored_results
        except Exception as e:
            logger.error(f"Failed to search in {collection_name}: {e}")
            return []

    async def batch_search(
        self,
        collection_name: str,
        query_texts: List[str],
        limit: Optional[int],
        with_vectors: bool = False,
    ):
        """
        Perform batch search using multiple query texts.

        Parameters:
        -----------
        - collection_name (str): Name of the collection.
        - query_texts (List[str]): List of query texts.
        - limit (Optional[int]): Max results per query.
        - with_vectors (bool): Include vectors in results (default False).

        Returns:
        --------
        - List[List[ScoredResult]]: List of result lists for each query.
        """
        return await asyncio.gather(
            *[
                self.search(
                    collection_name=collection_name,
                    query_text=query_text,
                    query_vector=None,
                    limit=limit,
                    with_vector=with_vectors,
                )
                for query_text in query_texts
            ]
        )

    async def delete_data_points(self, collection_name: str, data_point_ids: list[str]):
        """
        Delete data points from a collection by IDs.

        Parameters:
        -----------
        - collection_name (str): Name of the collection.
        - data_point_ids (list[str]): List of data point IDs to delete.

        Returns:
        --------
        - bool: True on success.
        """
        try:
            await self._query(
                "CogneeDeleteDataPoints",
                {"collection_name": collection_name, "dp_ids": data_point_ids},
            )
            return True
        except Exception as e:
            logger.error(f"Failed to delete data points from {collection_name}: {e}")
            raise

    async def prune(self):
        try:
            await self._query("CogneePruneCollections", {})
            return True
        except Exception as e:
            logger.error(f"Failed to prune collections: {e}")
            raise

    async def get_connection(self) -> helix.Client:
        """
        Get the HelixDB connection if it exists, otherwise initialize it.

        Returns:
        --------
            - HelixDB Client: The HelixDB connection object.
        """
        if self.connection is None:
            self._initialize_connection()

        return cast(helix.Client, self.connection)

    async def create_vector_index(self, index_name: str, index_property_name: str):
        """
        In HelixDB, collections are created implicitly when data is added,
        so this is a no-op placeholder for interface compatibility.
        """

        # HelixDB creates collections implicitly when first data point is added
        # No explicit collection creation needed
        logger.debug(f"Collection {index_name}_{index_property_name} will be created implicitly")
        pass

    async def index_data_points(
        self, index_name: str, index_property_name: str, data_points: list[DataPoint]
    ):
        """
        Index data points by extracting a specific property and storing simplified records.

        Parameters:
        -----------
        - index_name (str): The data point type name (e.g., "Entity", "TextDocument")
        - index_property_name (str): The property to extract and index (e.g., "name", "text")
        - data_points (list[DataPoint]): List of data points to index
        """

        # Define the simplified schema for indexed data
        class IndexSchema(DataPoint):
            id: str
            text: str
            metadata: dict = {"index_fields": ["text"]}

        # Transform complex data points into simplified indexed records
        await self.create_data_points(
            f"{index_name}_{index_property_name}",
            [
                IndexSchema(
                    id=str(data_point.id),
                    text=getattr(data_point, data_point.metadata["index_fields"][0]),
                )
                for data_point in data_points
            ],
        )

    async def get_all_collections(self):
        """
        Get all vector collections in the HelixDB database.

        Returns:
        --------
            - list[str]: A list of all vector collections in the HelixDB database.
        """
        try:
            collections = await self._query("CogneeGetCollections", {})

            if isinstance(collections, dict):
                return collections.get("collections")

            return []
        except Exception as e:
            logger.error(f"Failed to get all collections: {e}")
            raise

    async def _get_collection_by_name(self, collection_name: str):
        """
        Get a collection by name.

        Parameters:
        -----------
            - collection_name (str): The name of the collection to get.

        Returns:
        --------
            - dict: The collection object.
        """
        try:
            result = await self._query("CogneeGetCollection", {"collection_name": collection_name})

            if isinstance(result, dict):
                collection = result.get("collection")
                if collection:
                    return collection

            return None
        except Exception as e:
            logger.error(f"Failed to get collection by name {collection_name}: {e}")
            raise

    async def _create_data_point(
        self, collection_name: str, vector: list[float], dp_id: str, payload: str, content: str
    ):
        """
        Create a single data point.

        Parameters:
        -----------
        - collection_name (str): Name of the collection.
        - vector (list[float]): Vector representation of the data point.
        - dp_id (str): Data point ID.
        - payload (str): Payload of the data point as a JSON string.
        - content (str): Content of the data point.
        """
        try:
            await self._query(
                "CogneeCreateDataPoint",
                {
                    "collection_name": collection_name,
                    "vector": vector,
                    "dp_id": dp_id,
                    "payload": payload,
                    "content": content,
                },
            )
        except Exception as e:
            logger.error(f"Failed to create data point in {collection_name}: {e}")
            raise

    def _parse_doc_to_scored_result(self, doc: Dict[str, Any], score: float = 0) -> ScoredResult:
        """
        Parse a document dict from HelixDB API into a ScoredResult.

        Parameters:
        -----------
        - doc (Dict[str, Any]): Document dict with data_point_id, payload, etc.
        - score (float): Score for the result (default 0).

        Returns:
        --------
        - ScoredResult: Parsed result with UUID id, payload dict, and score.
        """

        id_str = str(doc["data_point_id"])
        payload_str = str(doc["payload"])
        payload = json.loads(payload_str)

        return ScoredResult(id=cast(UUID, parse_id(id_str)), payload=payload, score=score)
