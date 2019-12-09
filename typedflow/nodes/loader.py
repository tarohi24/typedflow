from __future__ import annotations
from functools import wraps
from dataclasses import dataclass, field
import logging
from typing import (
    get_args,
    get_type_hints,
    Callable,
    Iterator,
    Iterable,
    Generator,
    List,
    Type,
)

from typedflow.batch import Batch
from typedflow.exceptions import EndOfBatch
from typedflow.types import K

from . import ProviderNode


__all__ = ['LoaderNode', ]
logger = logging.getLogger(__file__)


@dataclass
class LoaderNode(ProviderNode[K]):
    """
    caution: We don't offer any interfaces to accept generator.
    We only accept generative function because we cannot obtain any
    typing information from a genarator.
    This behavior may change if Python supports getting typings from
    a generator
    """
    batch_size: int = 16
    itr: Iterator[K] = field(init=False)

    def __post_init__(self):
        ProviderNode.__post_init__(self)
        self.itr: Iterator[K] = iter(self.func())

    def get_return_type(self) -> Type[K]:
        typ: Type[Iterable[K]] = get_type_hints(self.func)['return']
        try:
            return get_args(typ)[0]
        except IndexError:
            raise AssertionError(f'function {self.func.__name__} may not return iterbale')

    def load(self) -> Generator[Batch[K], None, None]:
        lst: List[K] = []
        batch_id: int = 0
        while True:
            for _ in range(self.batch_size):
                try:
                    item: K = next(self.itr)
                except StopIteration:
                    batch: Batch[K] = Batch[K](batch_id=batch_id, data=lst)
                    if len(batch.data) > 0:
                        yield batch
                    return
                lst.append(item)
            batch: Batch[K] = Batch[K](batch_id=batch_id, data=lst)
            yield batch
            batch_id += 1
            lst: List[K] = []  # noqa

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[K]:
        """
        CAUTION: for unit testing of this method, we have to set
        cache_table.life = 1 in advance (defualt 0)
        """
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            try:
                batch: Batch[K] = next(self.load())
            except StopIteration:
                raise EndOfBatch()
            self.cache_table.set(key=batch_id, value=batch)
            return self.cache_table.get(batch_id)
