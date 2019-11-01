from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Generic, Generator, List, Type, TypeVar

from dataclasses_json import dataclass_json


T = TypeVar('T')  # serializable
K = TypeVar('K')  # serializable


@dataclass_json
@dataclass
class Batch(Generic[T]):
    batch_id: int
    data: List[T]


@dataclass
class Task(Generic[T, K]):
    in_type: Type = T
    out_type: Type = K
    func: Callable[[T], K]

    def process(self,
                batch: Batch[T]) -> Batch[K]:
        lst: List[K] = [self.func(item) for item in batch.data]
        return Batch(data=lst)


@dataclass
class DataLoarder(Generic[K]):
    out_type: Type = K
    gen: Generator[K, None, None]
    batch_size: int = 16

    def load(self) -> Generator[Batch[K], None, None]:
        lst: List[K] = []
        batch_id: int = 0
        while True:
            for _ in range(self.batch_size):
                try:
                    item: K = next(self.gen)
                except StopIteration:
                    return lst
                lst.append(item)
            batch: Batch[K] = Batch[K](batch_id=batch_id, data=lst)
            yield batch


@dataclass
class Dumper(Generic[T]):
    in_type: Type = T
    func: Callable[Batch[T], None]  # dumping function

    def dump(self, batch: Batch[T]) -> None:
        self.func(batch)


@dataclass
class Pipeline:
    loader: DataLoarder
    pipeline: List[Task]
    dumper: Dumper

    def validate(self) -> None:
        assert len(self.pipeline) > 0, 'No tasks are in the pipeline'
        assert self.loader.out_type == self.pipeline[0].in_type
        for prev, nxt in zip(self.pipeline, self.pipeline[1:]):
            assert prev.out_type == nxt.in_type
        assert self.pipeline[-1].out_type == self.dumper.in_type

    def run(self,
            validate: bool = True) -> None:
        """
        Return
        -----
        exit code
        """
        def _run(batch: Batch, tasks: List[Task]) -> Batch:
            if len(tasks) == 0:
                return batch
            else:
                head, *tail = tasks
                return _run(head(batch), tail)

        if validate:
            self.validate()
        for batch in self.loader.load:
            self.dumper.dump(batch)
