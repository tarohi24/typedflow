from typing import Generator, List, TypedDict

import pytest

from typedflow.exceptions import EndOfBatch
from typedflow.nodes import LoaderNode, TaskNode


def load() -> List[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    return lst


@pytest.fixture
def loader_node() -> LoaderNode[str]:
    node: LoaderNode[str] = LoaderNode(func=load,
                                       batch_size=2)
    return node


class SampleArg(TypedDict):
    s: str
    i: int


def test_load():
    def gen() -> Generator[int, None, None]:
        for i in range(3):
            yield SampleArg(s='hi', i=i)
    # first batch
    loader: LoaderNode[int] = LoaderNode(func=gen,
                                         batch_size=2)
    load = loader.load()
    batch_1 = next(load)
    assert batch_1.batch_id == 0
    strs = [a['s'] for a in batch_1.data]
    ints = [a['i'] for a in batch_1.data]
    assert strs == ['hi', 'hi']
    assert ints == [0, 1]

    # second batch
    batch_2 = next(load)
    assert batch_2.batch_id == 1
    strs = [a['s'] for a in batch_2.data]
    ints = [a['i'] for a in batch_2.data]
    assert strs == ['hi', ]
    assert ints == [2, ]

    # no more batch
    with pytest.raises(StopIteration):
        next(loader.load())
    with pytest.raises(StopIteration):
        next(loader.load())


def test_get_or_produce_batch(loader_node):
    node = loader_node
    node.add_succ()
    batch = node.get_or_produce_batch(0)
    assert batch.data == ['hi', 'hello']
    batch = node.get_or_produce_batch(1)  # noqa
    assert batch.data == ['konnichiwa']


def test_cache(loader_node):
    node = loader_node
    node.add_succ()
    node.add_succ()
    batch = node.get_or_produce_batch(0)
    assert batch.data == ['hi', 'hello']
    assert node.cache_table.cache_table[0].value.data == ['hi', 'hello']
    batch = node.get_or_produce_batch(0)  # noqa
    assert batch.data == ['hi', 'hello']
    assert 0 not in node.cache_table.cache_table


def test_loader_with_multiple_targets():
    def load() -> Generator[str, None, None]:
        return ['hi']

    def task_func_1(s: str) -> str:
        return s

    def task_func_2(s: str) -> str:
        return s

    def merge_str(a: str, b: str, c: str) -> str:
        return s

    loader: LoaderNode[str] = LoaderNode(func=load)
    first_task: TaskNode[str] = TaskNode(func=task_func_1)
    (first_task < loader)('s')
    second_task: TaskNode[str] = TaskNode(func=task_func_2)
    (second_task < first_task)('s')
    last_task: TaskNode[str] = TaskNode(func=merge_str)
    (last_task < first_task)('a')
    (last_task < loader)('b')
    (last_task < second_task)('c')

    # succ counts asssertions
    assert loader._succ_count == 2
    assert first_task._succ_count == 2
    assert second_task._succ_count == 1
    last_task.accept(batch_id=0)
    with pytest.raises(EndOfBatch):
        last_task.accept(batch_id=0)
