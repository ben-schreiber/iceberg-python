from __future__ import annotations
from functools import cached_property
from typing import List, Optional, Set, Tuple, Union, TYPE_CHECKING
from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableRequest, CommitTableResponse, Table
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from pyiceberg.exceptions import NoSuchTableError
from pynessie.client.nessie_client import NessieClient
from pynessie.model import Delete
from pynessie.error import NessieContentNotFoundException

if TYPE_CHECKING:
    import pyarrow as pa


def _split_table_and_branch(name: str) -> Tuple[str, Optional[str]]:
    table_name, _, branch_name = name.partition("@")
    return table_name, branch_name or None


class NessieCatalog(Catalog):

    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        self._client = NessieClient()

    @cached_property
    def _default_branch(self) -> str:
        return self._client.get_default_branch()

    def _get_branch_and_hash(self, branch_name: Optional[str]) -> Tuple[str, str]:
        """
        Return the branch name and its corresponding hash. If no branch name is passed,
        then the default branch is used.
        """
        branch_ref = self._client.get_reference(branch_name or self._default_branch)
        return branch_ref.name, branch_ref.hash_
    
    def _parse_identifier(self, identifier: Union[str, Identifier]) -> Tuple[str, str, str, str]:
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        database_name, table_name_and_branch = self.identifier_to_database_and_table(identifier_tuple, NoSuchTableError)
        table_name, branch_name = _split_table_and_branch(table_name_and_branch)
        branch_name, branch_hash = self._get_branch_and_hash(branch_name)
        return database_name, table_name, branch_name, branch_hash


    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, pa.Schema],
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        pass

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        pass

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        pass

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        database_name, table_name, branch_name, branch_hash = self._parse_identifier(identifier)
        try:
            self._client.commit(
                branch_name,
                branch_hash,
                None,
                None,
                Delete(f'{database_name}.{table_name}')
            )

        except NessieContentNotFoundException as exc:
            raise NoSuchTableError(f"Table does not exist {database_name}.{table_name}") from exc

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        pass

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        pass

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        pass

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        pass

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        pass

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        pass

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        pass

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        pass
