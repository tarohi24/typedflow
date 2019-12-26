from . import ProviderNode
from typedflow.batch import Batch
from typedflow.types import K
from typing import Any, Generator, Iterator, Type

class LoaderNode(ProviderNode[K]):
    batch_size: int = ...
    itr: Iterator[K] = ...
    def __post_init__(self) -> None: ...
    def get_return_type(self) -> Type[K]: ...
    def load(self) -> Generator[Batch[K], None, None]: ...
    def get_or_produce_batch(self, batch_id: int) -> Batch[K]: ...
    def __init__(self, func: Any, debug: Any, batch_size: Any) -> None: ...
