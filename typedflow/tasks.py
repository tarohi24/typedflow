from dataclasses import dataclass
import logging
from typing import (
    Callable, Generic, Generator, Iterable, Iterator, List)

from typedflow.batch import Batch
from typedflow.exceptions import EndOfBatch, FaultItem
from typedflow.types import T, K


__all__ = ['Task', 'DataLoader']
logger = logging.getLogger(__file__)


@dataclass
class Task(Generic[T, K]):
    func: Callable[[T], K]

    def process(self,
                batch: Batch[T]) -> Batch[K]:
        if len(batch.data) == 0:
            raise EndOfBatch()
        products: List[K] = []
        for item in batch.data:
            if isinstance(item, FaultItem):
                continue
            try:
                products.append(self.func(item))
            except Exception as e:
                logger.warn(repr(e))
                products.append(FaultItem())
        return Batch[K](batch_id=batch.batch_id,
                        data=products)


@dataclass
class DataLoader(Generic[K]):
    gen: Iterable[K]
    batch_size: int = 16

    def load(self) -> Generator[Batch[K], None, None]:
        lst: List[K] = []
        batch_id: int = 0
        itr: Iterator[K] = iter(self.gen)
        while True:
            for _ in range(self.batch_size):
                try:
                    item: K = next(itr)
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
