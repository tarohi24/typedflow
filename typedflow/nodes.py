from __future__ import annotations
from dataclasses import dataclass, field
import logging
from typing import (
    Dict,
    Generic,
    List,
    Type,
    TypedDict,
    TypeVar
)

from typedflow.batch import Batch
from typedflow.counted_cache import CacheTable
from typedflow.exceptions import EndOfBatch
from typedflow.tasks import (
    Task, DataLoader, Dumper
)
from typedflow.types import T, K


__all__ = ['DumpNode']


logger = logging.getLogger(__file__)
TD = TypeVar('TD', bound=TypedDict)


@dataclass
class ProviderNode(Generic[K]):
    """
    LoaderNode or TaskNode
    """
    succ_count: int = field(init=False)
    cache_table: CacheTable[int, Batch[K]] = field(init=False)

    def __post_init__(self):
        self.succ_count: int = 0
        self.cache_table: CacheTable[int, Batch[K]]\
            = CacheTable[int, Batch[K]](life=0)

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[K]:
        ...

    def add_succ(self):
        self.succ_count += 1
        self.cache_table.life += 1


@dataclass
class ConsumerNode(Generic[T]):
    """
    TaskNode or DumpNode
    Note: this is not defined as a dataclass due to the inheritance problem
    see: https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses  # noqa
    """
    arg_type: Type[T] = field(init=False)  # must be same as T
    precs: Dict[str, ProviderNode[T]] = field(init=False)

    def __post_init__(self, arg_type: Type[T]):
        self.arg_type: Type[T] = arg_type  # must be same as T
        self.precs: Dict[str, ProviderNode[T]] = dict()

    def set_upstream_node(self,
                          key: str,
                          node: ProviderNode[T]) -> None:
        self.precs[key] = node
        node.succ_count += 1

    def _merge_batches(self,
                       materials: Dict[str, Batch]) -> Batch[TD]:
        """
        TD is the same class as self.arg_type
        """
        material_data: Dict[str, List] = {key: batch.data
                                          for key, batch
                                          in self.materials.items()}
        keys, values = list(zip(*material_data.items()))
        data: List[TD] = [
            self.arg_type({key: list(val) for key in keys})
            for val in zip(*values)
        ]
        batch = Batch(batch_id=self.batch_id, data=data)
        return batch

    async def acecpt(self,
                     batch_id: int) -> Batch[T]:
        """
        merge all the arguments items into an instance of T (=arg_type)
        """
        if len(self.precs) == 1:
            prec: ProviderNode[T] = next(iter(self.prec.values()))
            product: Batch[T] = await prec.get_or_produce_batch(batch_id=batch_id)
            return product
        else:
            assert issubclass(self.arg_type, TypedDict)
            materials: Dict[str, Batch] = {
                key: await prec.get_or_produce_batch(batch_id=batch_id)
                for key, prec in self.precs.items()
            }
            merged_batch: Batch[T] = self._merge_batches(materials=materials)
            return merged_batch


@dataclass
class LoaderNode(ProviderNode[K]):
    loader: DataLoader[K]

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[K]:
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            try:
                batch: Batch[K] = self.loarder.load()
            except StopIteration:
                return EndOfBatch()
            self.cache_table.set(key=batch_id, value=batch)
            return self.cache_table.get(batch_id)


@dataclass
class TaskNode(ProviderNode[K], ConsumerNode[T]):
    task: Task[T, K]

    async def get_or_produce_batch(self,
                                   batch_id: int) -> Batch[K]:
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            arg: Batch[T] = await self.funnel(batch_id=batch_id)
            product: Batch[K] = self.task.process(arg)
            return product


@dataclass
class DumpNode(ConsumerNode[T]):
    dumper: Dumper[T]
    finished: bool = False

    async def run_and_dump(self,
                           batch_id: int) -> None:
        if not self.finished:
            try:
                batch: Batch = await self.prec.get_or_produce_batch(
                    batch_id=batch_id)
                self.dumper.dump(batch)
            except EndOfBatch:
                self.finished: bool = True
        else:
            return
