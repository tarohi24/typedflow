from __future__ import annotations
from dataclasses import dataclass, field
import logging
from typing import (
    Dict,
    Generic,
    List,
    Type,
)

from typedflow.batch import Batch
from typedflow.counted_cache import CacheTable
from typedflow.exceptions import EndOfBatch
from typedflow.tasks import (
    Task, DataLoader, Dumper
)
from typedflow.types import A, B


__all__ = ['DumpNode']


logger = logging.getLogger(__file__)


@dataclass
class Funnel(Generic[A]):
    """
    Genetetor of a new batch from multiple batches
    """
    materials: Dict[str, Batch]
    target_cls: Type[A]  # same as A
    batch_id: int = field(init=False)

    def __post_init__(self):
        batch_ids: List[int] = [batch.batch_id
                                for batch in self.materials.values()]
        assert all([id_ == batch_ids[0] for id_ in batch_ids[1:]])
        self.batch_id: int = batch_ids[0]

    def merge_batches(self) -> Batch[A]:
        material_data: Dict[str, List] = {key: batch.data
                                          for key, batch
                                          in self.materials.items()}
        keys, values = list(zip(*material_data.items()))
        data: List[A] = [
            self.target_cls(**{key: list(val) for key in keys})
            for val in zip(*values)
        ]
        batch = Batch(batch_id=self.batch_id, data=data)
        return batch


class ProviderNode(Generic[B]):
    """
    LoaderNode or TaskNode
    """
    def __init__(self):
        self.succ_count: int = 0
        self.cache_table: CacheTable[int, Batch[B]]\
            = CacheTable[int, Batch[B]](life=0)

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[B]:
        raise NotImplementedError()

    def add_succ(self):
        self.succ_count += 1
        self.cache_table.life += 1


class ConsumerNode(Generic[A]):
    """
    TaskNode or DumpNode
    Note: this is not defined as a dataclass due to the inheritance problem
    see: https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses  # noqa
    """
    def __init__(self, arg_type: Type[A]):
        self.arg_type: Type[A] = arg_type  # must be same as A
        self.precs: Dict[str, ProviderNode[A]] = dict()

    def set_upstream_node(self,
                          key: str,
                          node: ProviderNode[A]) -> None:
        self.precs[key] = node
        node.succ_count += 1

    async def funnel(self,
                     batch_id: int) -> Batch[A]:
        """
        merge all the arguments items into an instance of A (=arg_type)
        """
        materials: Dict[str, Batch] = {
            key: await prec.get_or_produce_batch(batch_id=batch_id)
            for key, prec in self.precs.items()
        }
        funnel: Funnel[A] = Funnel[A](materials=materials,
                                      target_cls=self.arg_type)
        merged_batch: Batch[A] = funnel.merge_batches()
        return merged_batch


@dataclass
class LoaderNode(ProviderNode[B]):
    loader: DataLoader[B]

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[B]:
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            try:
                batch: Batch[B] = self.loarder.load()
            except StopIteration:
                return EndOfBatch()
            self.cache_table.set(key=batch_id, value=batch)
            return self.cache_table.get(batch_id)


@dataclass
class TaskNode(ProviderNode[B], ConsumerNode[A]):
    task: Task[A, B]

    async def get_or_produce_batch(self,
                                   batch_id: int) -> Batch[B]:
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            arg: Batch[A] = await self.funnel(batch_id=batch_id)
            product: Batch[B] = self.task.process(arg)
            return product


@dataclass
class DumpNode(ConsumerNode[A]):
    dumper: Dumper[A]
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
