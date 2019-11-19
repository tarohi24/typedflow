import asyncio
from typing import List

import pytest

from typedflow.nodes import LoaderNode


@pytest.fixture
def loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    node: LoaderNode[str] = LoaderNode(orig=lst, batch_size=2)
    return node


def test_load():
    gen: Generator[int, None, None] = (SampleArg(s='hi', i=i) for i in range(3))
    # first batch
    loader: LoaderNode[int] = LoaderNode(orig=gen, batch_size=2)
    batch_1 = next(loader.load())
    assert batch_1.batch_id == 0
    strs = [a['s'] for a in batch_1.data]
    ints = [a['i'] for a in batch_1.data]
    assert strs == ['hi', 'hi']
    assert ints == [0, 1]

    # second batch
    batch_2 = next(loader.load())
    assert batch_2.batch_id == 1
    strs = [a['s'] for a in batch_2.data]
    ints = [a['i'] for a in batch_2.data]
    assert strs == ['hi', ]
    assert ints == [2, ]

    # no more batch
    with pytest.raises(StopIteration):
        next(iter(gen))
    with pytest.raises(StopIteration):
        next(iter(gen))


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
