from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import (
    Callable,
    Generic,
    Generator,
    Iterable,
    Iterator,
    List,
    Union,
    TypeVar
)


logger = logging.getLogger(__file__)


T = TypeVar('T')  # serializable
K = TypeVar('K')  # serializable


class BatchIsEmpty(Exception):
    pass


class FaultItem:
    pass


@dataclass
class Batch(Generic[T]):
    batch_id: int
    data: List[Union[T, FaultItem]]


@dataclass
class Task(Generic[T, K]):
    func: Callable[[T], K]

    def process(self,
                batch: Batch[T]) -> Batch[K]:
        products: List[K] = []
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
            return Batch[K](batch_id=batch.batch_id,
                            data=products)
        else:
            raise BatchIsEmpty()


@dataclass
class DataLoader(Generic[K]):
    gen: Iterable[K, None, None]
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


@dataclass
class Dumper(Generic[T]):
    func: Callable[Batch[T], None]  # dumping function

    def dump(self, batch: Batch[T]) -> None:
        self.func(batch)


@dataclass
class Pipeline:
    loader: DataLoader
    pipeline: List[Task]
    dumper: Dumper

    def validate(self) -> None:
        """
        not implemented because Python Generic doesn't offer
        any ways to access to the actual type
        """
        pass

    def run(self,
            validate: bool = True) -> None:

        def _run(batch: Batch, tasks: List[Task]) -> Batch:
            if len(tasks) == 0:
                return batch
            else:
                head, *tail = tasks
                next_batch: Batch = head.process(batch)
                return _run(next_batch, tail)

        if validate:
            self.validate()
        for batch in self.loader.load():
            try:
                product: Batch = _run(batch, self.pipeline)
            except BatchIsEmpty:
                continue
            self.dumper.dump(product)
