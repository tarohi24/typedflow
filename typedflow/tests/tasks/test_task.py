from typing import Generator, List

import pytest

from typedflow.arguments import TaskArguments
from typedflow.batch import Batch
from typedflow.defaults import IntArgument, StringArguemnt
from typedflow.tasks import Task


@pytest.fixture
def task() -> Task:
    def count_chars(s: StringArguemnt) -> IntArgument:
        return IntArgument(len(s.value))
    task: Task = Task[StringArguemnt, IntArgument](func=count_chars)
    return task


@pytest.fixture
def gen_batches() -> Generator[Batch[StringArguemnt], None, None]:
    strs: List[StringArguemnt] = [StringArguemnt(s) for s in ['hi', 'hello']]
    for i in range(2):
        yield Batch(batch_id=i, data=strs)


def test_task(task, gen_batches):
    mat: Batch[StringArguemnt] = next(gen_batches)
    prod: Batch[IntArgument] = task.process(batch)
    assert prod.data == [2, 5]
