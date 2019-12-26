from typedflow.nodes import ConsumerNode, DumpNode, LoaderNode, ProviderNode
from typing import Any, List, Type, Union

Node = Union[ProviderNode, ConsumerNode]

class Flow:
    dump_nodes: List[DumpNode]
    debug: bool = ...
    def validate(self) -> None: ...
    def get_loader_nodes_with_broadcast(self) -> List[LoaderNode]: ...
    def run(self, validate: bool=...) -> None: ...
    def is_inherited(self, sub: Type, sup: Type) -> bool: ...
    def typecheck(self) -> None: ...
    def __init__(self, dump_nodes: Any, debug: Any) -> None: ...
