from typing import List

import pytest

from typedflow.nodes import LoaderNode


@pytest.fixture
def loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    node: LoaderNode[str] = LoaderNode(orig=lst,
                                       return_type=str,
                                       batch_size=2)
    return node


def test_init(loader_node):
    node = loader_node
    assert node._succ_count == 0
    assert len(node.cache_table) == 0


def test_add_succ(loader_node):
    node = loader_node
    node.add_succ()
    node.add_succ()
    assert node._succ_count == 2
    assert len(node.cache_table) == 0
    assert node.cache_table.life == 2
