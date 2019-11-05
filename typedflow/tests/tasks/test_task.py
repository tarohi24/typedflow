from typing import Generator, List

import pytest

from typedflow.arguments import TaskArguments
from typedflow.batch import Batch
from typedflow.defaults import IntArgument, StrArgument
from typedflow.tasks import Task


@pytest.fixture
def task() -> Task:
    def count_chars(s: StrArgument) -> IntArgument:
        return IntArgument(len(s.value))
    task: Task = Task[StrArgument, IntArgument](func=count_chars)
    return task


def gen_batches() -> Generator[Batch[StrArgument], None, None]:
    strs: List[StrArgument] = [StrArgument(s) for s in ['hi', 'hello']]
    for i in range(2):
        yield Batch(batch_id=i, data=strs)


def test_task(task):
    gen: Generator[Batch[StrArgument], None, None] = gen_batches()
    mat: Batch[StrArgument] = next(gen)
    prod: Batch[IntArgument] = task.process(mat)
    assert [item.value for item in prod.data] == [2, 5]
