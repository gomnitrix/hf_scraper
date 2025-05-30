from typing import Dict, Any, List, Optional
import pydgraph
import json
import logging
from datetime import datetime

class DgraphClient:
    def __init__(self, dgraph_url: str = "localhost:9080"):
        self.client_stub = pydgraph.DgraphClientStub(dgraph_url)
        self.client = pydgraph.DgraphClient(self.client_stub)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __del__(self):
        """Clean up resources."""
        if hasattr(self, 'client_stub'):
            self.client_stub.close()

    async def alter_schema(self, schema: str) -> None:
        """Alter the Dgraph schema."""
        try:
            op = pydgraph.Operation(schema=schema)
            self.client.alter(op)
        except Exception as e:
            raise Exception(f"Failed to alter schema: {str(e)}")

    async def mutate(self, mutation: str) -> Dict[str, Any]:
        """Execute a mutation."""
        try:
            txn = self.client.txn()
            try:
                mu = pydgraph.Mutation(set_nquads=mutation.encode('utf8'))
                response = txn.mutate(mu)
                txn.commit()
                return response
            except Exception as e:
                txn.discard()
                raise e
        except Exception as e:
            raise Exception(f"Failed to execute mutation: {str(e)}")

    async def upsert(self, query: str, mutation: str) -> Dict[str, Any]:
        """Execute an upsert operation (query + mutation)."""
        try:
            txn = self.client.txn()
            try:
                mu = pydgraph.Mutation(set_nquads=mutation.encode('utf8'))
                request = txn.create_request(query=query, mutations=[mu], commit_now=True)
                response = txn.do_request(request)
                return response
            except Exception as e:
                txn.discard()
                raise e
        except Exception as e:
            raise Exception(f"Failed to execute upsert: {str(e)}")

    async def query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a query."""
        try:
            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query, variables=variables)
                return json.loads(response.json)
            finally:
                txn.discard()
        except Exception as e:
            raise Exception(f"Failed to execute query: {str(e)}")

    def _create_upsert_query(self, type_name: str, name_field: str, name_value: str) -> str:
        """Create an upsert query to find existing node."""
        return f"""{{
            u as var(func: eq({name_field}, "{name_value}")) @filter(type({type_name}))
        }}"""

    def _create_upsert_mutation(self, data: Dict[str, Any], type_name: str) -> str:
        """Create an upsert mutation using uid function."""
        rdf_lines = []
        uid = "uid(u)"
        
        # Add type if node is new
        rdf_lines.append(f"{uid} <dgraph.type> \"{type_name}\" .")
        
        # Add scalar predicates
        for key, value in data.items():
            if key == 'id':  # Skip id field as it's used in the query
                continue
            if isinstance(value, (str, int, float, bool)):
                if isinstance(value, str):
                    value = f'"{value}"'
                rdf_lines.append(f"{uid} <{key}> {value} .")
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        rdf_lines.append(f"{uid} <{key}> \"{item}\" .")
                    else:
                        rdf_lines.append(f"{uid} <{key}> {item} .")
            elif isinstance(value, dict):
                # Handle nested objects
                for nested_key, nested_value in value.items():
                    if isinstance(nested_value, (str, int, float, bool)):
                        if isinstance(nested_value, str):
                            nested_value = f'"{nested_value}"'
                        rdf_lines.append(f"{uid} <{key}.{nested_key}> {nested_value} .")
        
        return "\n".join(rdf_lines)

    async def upsert_model(self, model_data: Dict[str, Any]) -> None:
        """Upsert a model into Dgraph."""
        query = self._create_upsert_query("Model", "name", model_data["name"])
        mutation = self._create_upsert_mutation(model_data, "Model")
        await self.upsert(query, mutation)

    async def upsert_dataset(self, dataset_data: Dict[str, Any]) -> None:
        """Upsert a dataset into Dgraph."""
        query = self._create_upsert_query("Dataset", "name", dataset_data["name"])
        mutation = self._create_upsert_mutation(dataset_data, "Dataset")
        await self.upsert(query, mutation)

    async def upsert_organization(self, org_data: Dict[str, Any]) -> None:
        """Upsert an organization into Dgraph."""
        query = self._create_upsert_query("Organization", "name", org_data["name"])
        mutation = self._create_upsert_mutation(org_data, "Organization")
        await self.upsert(query, mutation)

    async def upsert_collection(self, collection_data: Dict[str, Any]) -> None:
        """Upsert a collection into Dgraph."""
        query = self._create_upsert_query("Collection", "name", collection_data["name"])
        mutation = self._create_upsert_mutation(collection_data, "Collection")
        await self.upsert(query, mutation)

    async def upsert_user(self, user_data: Dict[str, Any]) -> None:
        """Upsert a user into Dgraph."""
        query = self._create_upsert_query("User", "name", user_data["name"])
        mutation = self._create_upsert_mutation(user_data, "User")
        await self.upsert(query, mutation)

    async def upsert_license(self, license_data: Dict[str, Any]) -> None:
        """Upsert a license into Dgraph."""
        query = self._create_upsert_query("License", "name", license_data["name"])
        mutation = self._create_upsert_mutation(license_data, "License")
        await self.upsert(query, mutation)

    async def upsert_tag(self, tag_data: Dict[str, Any]) -> None:
        """Upsert a tag into Dgraph."""
        query = self._create_upsert_query("Tag", "name", tag_data["name"])
        mutation = self._create_upsert_mutation(tag_data, "Tag")
        await self.upsert(query, mutation)

    async def upsert_library(self, library_data: Dict[str, Any]) -> None:
        """Upsert a library into Dgraph."""
        query = self._create_upsert_query("Library", "name", library_data["name"])
        mutation = self._create_upsert_mutation(library_data, "Library")
        await self.upsert(query, mutation)

    async def _get_node_uid(self, type_name: str, name: str) -> str:
        """Get the UID of a node by its name."""
        query = f"""{{
            node(func: eq(name, "{name}")) @filter(type({type_name})) {{
                uid
            }}
        }}"""
        result = await self.query(query)
        if result.get("node") and len(result["node"]) > 0:
            return result["node"][0]["uid"]
        return None

    async def create_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """Create relationships between nodes."""
        rdf_lines = []
        for rel in relationships:
            try:
                # Get UIDs for both nodes
                from_uid = await self._get_node_uid(rel.get("from_type", "Model"), rel["from_name"])
                to_uid = await self._get_node_uid(rel.get("to_type", "Model"), rel["to_name"])
                if not from_uid or not to_uid:
                    self.logger.error(f"Skipping relationship: Node of type {rel.get('from_type', 'Model')} with name {rel['from_name']} or {rel.get('to_type', 'Model')} with name {rel['to_name']} not found")
                    continue
                
                # Create relationship using actual UIDs
                rdf_lines.append(f"<{from_uid}> <{rel['predicate']}> <{to_uid}> .")
            except Exception as e:
                self.logger.error(f"Failed to create relationship: {str(e)}")
                continue
        
        if rdf_lines:
            await self.mutate("\n".join(rdf_lines))

    async def delete_nodes(self, uids: List[str], batch_size: int = 1000) -> None:
        """Delete nodes by their UIDs in batches."""
        if not uids:
            return

        for i in range(0, len(uids), batch_size):
            batch = uids[i:i + batch_size]
            txn = self.client.txn()
            try:
                delete_nquads = ""
                for uid in batch:
                    delete_nquads += f"<{uid}> * * .\n"
                
                txn.mutate(del_nquads=delete_nquads)
                txn.commit()
            except Exception as e:
                txn.discard()
                self.logger.error(f"Failed to delete batch: {str(e)}")
                continue