import asyncio
from typing import List, TypedDict

import pytest

from typedflow.batch import Batch
from typedflow.tasks import Dumper, DataLoader
from typedflow.nodes import DumpNode, LoaderNode


class IntStr(TypedDict):
    i: int
    s: str


@pytest.fixture
def str_loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    loader: DataLoader[str] = DataLoader(gen=lst, batch_size=2)
    node: LoaderNode[str] = LoaderNode(loader=loader)
    return node


@pytest.fixture
def int_loader_node() -> LoaderNode[int]:
    lst: int = [1, 2, 3]
    loader: DataLoader[int] = DataLoader(gen=lst, batch_size=2)
    node: LoaderNode[int] = LoaderNode(loader=loader)
    return node


@pytest.fixture
def str_dump_node() -> DumpNode[str]:
    def printer(batch: Batch[str]) -> None:
        print(batch.data)
    dumper: Dumper[str] = Dumper(func=printer)
    node: DumpNode[str] = DumpNode(dumper=dumper, arg_type=str)
    return node


@pytest.fixture
def int_str_dump_node() -> Dumper[IntStr]:
    def printer(batch: Batch[IntStr]) -> None:
        for item in batch.data:
            print(f'{str(item["i"])} {item["str"]}')
    dumper: Dumper[IntStr] = Dumper(func=printer)
    node: DumpNode[IntStr] = DumpNode(arg_type=IntStr, dumper=dumper)
    return node


def test_init_with_argtype():
    def printer(batch: Batch[IntStr]) -> None:
        for item in batch.data:
            print(f'{str(item["i"])} {item["str"]}')
    dumper: Dumper[IntStr] = Dumper(func=printer)
    with pytest.raises(TypeError):
        DumpNode[str](dumper)


def test_init(str_dump_node):
    node = str_dump_node
    assert node.arg_type == str
    assert len(node.precs) == 0


def test_set_upstream(str_dump_node, str_loader_node, int_loader_node):
    node = str_dump_node
    node.set_upstream_node('loader', str_loader_node)
    with pytest.raises(AssertionError):
        node.set_upstream_node('loader', str_loader_node)
    assert len(node.precs) == 1
    assert str_loader_node._succ_count == 1

    node.set_upstream_node('ld', int_loader_node)
    assert len(node.precs) == 2
    assert int_loader_node._succ_count == 1
    assert str_loader_node._succ_count == 1


def test_accept_with_merging(int_str_dump_node, int_loader_node, str_loader_node):
    # single batch
    node = int_str_dump_node
    node.set_upstream_node('i', int_loader_node)
    node.set_upstream_node('s', str_loader_node)
    asyncio.run(node.accept(batch_id=0))
