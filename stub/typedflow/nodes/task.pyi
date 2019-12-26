from . import ConsumerNode, ProviderNode
from typedflow.batch import Batch
from typedflow.exceptions import FaultItem
from typedflow.types import K
from typing import Any, Callable, Dict, Type, Union

class TaskNode(ConsumerNode, ProviderNode[K], Callable):
    def __init__(self, func: Callable[..., K]) -> Any: ...
    def get_return_type(self) -> Type[K]: ...
    def process(self, batch: Batch[Union[Dict[str, Any]], FaultItem]) -> Batch[K]: ...
    def get_or_produce_batch(self, batch_id: int) -> Batch[K]: ...
    def __lt__(self, another: ProviderNode) -> Callable[[str], None]: ...
    def __gt__(self, another: ConsumerNode) -> Callable[[str], None]: ...
