import asyncio
from typing import List

import pytest

from typedflow.nodes import LoaderNode
from typedflow.tasks import DataLoader


@pytest.fixture
def loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    loader: DataLoader[str] = DataLoader(gen=lst, batch_size=2)
    node: LoaderNode[str] = LoaderNode(loader=loader)
    return node


def test_get_or_produce_batch(loader_node):
    node = loader_node
    node.add_succ()
    batch = asyncio.run(node.get_or_produce_batch(0))
    assert batch.data == ['hi', 'hello']
    batch = asyncio.run(node.get_or_produce_batch(1))  # noqa
    assert batch.data == ['konnichiwa']


def test_cache(loader_node):
    node = loader_node
    node.add_succ()
    node.add_succ()
    batch = asyncio.run(node.get_or_produce_batch(0))
    assert batch.data == ['hi', 'hello']
    assert node.cache_table.cache_table[0].value.data == ['hi', 'hello']
    batch = asyncio.run(node.get_or_produce_batch(0))  # noqa
    assert batch.data == ['hi', 'hello']
    assert 0 not in node.cache_table.cache_table
