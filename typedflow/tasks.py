from dataclasses import dataclass
import logging
from typing import (
    Callable, Generic, Generator, Iterable, Iterator, List)

from typedflow.batch import Batch
from typedflow.exceptions import BatchIsEmpty, FaultItem
from typedflow.types import A, B


__all__ = ['Task', 'DataLoader', 'Dumper']
logger = logging.getLogger(__file__)


@dataclass
class Task(Generic[A, B]):
    func: Callable[[A], B]

    def process(self,
                batch: Batch[A]) -> Batch[B]:
        products: List[B] = []
        for item in batch.data:
            if isinstance(item, FaultItem):
                continue
            try:
                products.append(self.func(item))
            except Exception as e:
                logger.warn(repr(e))
                products.append(FaultItem())
                continue
        if len(products) > 0:
            return Batch[B](batch_id=batch.batch_id,
                            data=products)
        else:
            raise BatchIsEmpty()


@dataclass
class DataLoader(Generic[B]):
    gen: Iterable[B]
    batch_size: int = 16

    def load(self) -> Generator[Batch[B], None, None]:
        lst: List[B] = []
        batch_id: int = 0
        itr: Iterator[B] = iter(self.gen)
        while True:
            for _ in range(self.batch_size):
                try:
                    item: B = next(itr)
                except StopIteration:
                    batch: Batch[B] = Batch[B](batch_id=batch_id, data=lst)
                    if len(batch.data) > 0:
                        yield batch
                    return
                lst.append(item)
            batch: Batch[B] = Batch[B](batch_id=batch_id, data=lst)
            yield batch
            batch_id += 1
            lst: List[B] = []  # noqa


@dataclass
class Dumper(Generic[A]):
    func: Callable[[Batch[A]], None]  # dumping function

    def dump(self, batch: Batch[A]) -> None:
        self.func(batch)
