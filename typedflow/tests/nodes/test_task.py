import asyncio
from typing import List

import pytest

from typedflow.nodes import TaskNode, LoaderNode


@pytest.fixture
def str_loader_node() -> LoaderNode[str]:
    def lst() -> List[str]:
        return ['hi', 'hello', 'konnichiwa']
    node: LoaderNode[str] = LoaderNode(func=lst,
                                       batch_size=2)
    return node


@pytest.fixture
def tasknode(str_loader_node) -> TaskNode[str, int]:
    def count_chars(s: str) -> int:
        return len(s)
    node: TaskNode[str, int] = TaskNode(func=count_chars)
    node.set_upstream_node('s', str_loader_node)
    return node


def test_init(tasknode):
    assert len(tasknode.precs) == 1
    assert tasknode._succ_count == 0


def test_provide(tasknode):
    batch = asyncio.run(tasknode.get_or_produce_batch(batch_id=0))
    assert batch.data == [2, 5]
    batch = asyncio.run(tasknode.get_or_produce_batch(batch_id=1))  # noqa
    assert batch.data == [len('konnichiwa')]
