from typing import Generator, List

import pytest

from typedflow.batch import Batch
from typedflow.tasks import Task


@pytest.fixture
def task() -> Task:
    def count_chars(s: str) -> int:
        return len(s)
    task: Task = Task[str, int](func=count_chars)
    return task


def gen_batches() -> Generator[Batch[int], None, None]:
    strs: List[str] = ['hi', 'hello']
    for i in range(2):
        yield Batch(batch_id=i, data=strs)


def test_task(task):
    gen: Generator[Batch[str], None, None] = gen_batches()
    mat: Batch[str] = next(gen)
    prod: Batch[int] = task.process(mat)
    assert [item for item in prod.data] == [2, 5]
