from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Generic, Generator, Iterable, Iterator, List, Type, TypeVar


T = TypeVar('T')  # serializable
K = TypeVar('K')  # serializable


@dataclass
class Batch(Generic[T]):
    batch_id: int
    data: List[T]


@dataclass
class Task(Generic[T, K]):
    func: Callable[[T], K]

    def process(self,
                batch: Batch[T]) -> Batch[K]:
        lst: List[K] = [self.func(item) for item in batch.data]
        return Batch(batch_id=batch.batch_id, data=lst)


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
                    yield batch
                    return
                lst.append(item)
            batch: Batch[K] = Batch[K](batch_id=batch_id, data=lst)
            yield batch


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
                return _run(head.process(batch), tail)

        if validate:
            self.validate()
        for batch in self.loader.load():
            product: Batch = _run(batch, self.pipeline)
            self.dumper.dump(product)
