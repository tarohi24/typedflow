import asyncio
from typing import Generator, List, TypedDict

import pytest

from typedflow.batch import Batch
from typedflow.nodes import DumpNode, LoaderNode, TaskNode


class IntStr(TypedDict):
    s: str
    i: int



@pytest.fixture
def str_loader_node() -> LoaderNode[str]:
    def load_str() -> Generator[str, None, None]:
        lst: List[str] = ['hi', 'hello', 'konnichiwa']
        for item in lst:
            yield item
    node: LoaderNode[str] = LoaderNode(func=load_str,
                                       batch_size=2)
    return node


@pytest.fixture
def int_loader_node() -> LoaderNode[int]:
    def load_int() -> Generator[int, None, None]:
        lst: int = [1, 2, 3]
        for item in lst:
            yield item
    node: LoaderNode[str] = LoaderNode(func=load_int,
                                       batch_size=2)
    return node


@pytest.fixture
def str_dump_node() -> DumpNode[str]:
    def printer(s: str) -> None:
        print(s)
    node: DumpNode[str] = DumpNode(func=printer)
    return node


@pytest.fixture
def int_str_dump_node() -> DumpNode[IntStr]:
    def printer(item: IntStr) -> None:
        print(f'{str(item["i"])} {item["s"]}')
    node: DumpNode[IntStr] = DumpNode(func=printer)
    return node


def test_init(str_dump_node):
    node = str_dump_node
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


def test_batch_len_and_id(str_dump_node):
    node = str_dump_node
    batches = [
        Batch(batch_id=0, data=[1, 2]),
        Batch(batch_id=0, data=[2, 3]),
    ]
    assert node._get_batch_id(batches) == 0
    assert node._get_batch_len(batches) == 2
    batches.append(Batch(batch_id=1, data=[]))
    with pytest.raises(AssertionError):
        node._get_batch_id(batches)
    assert node._get_batch_len(batches) == 0


def test_merging(int_str_dump_node, int_loader_node, str_loader_node):
    node = int_str_dump_node
    batch_str = Batch(batch_id=0, data=['hi', 'hello'])
    batch_int = Batch(batch_id=0, data=[1, 2])
    materials = {'s': batch_str, 'i': batch_int}
    batch = node._merge_batches(materials)
    assert batch.data[0] == {'s': 'hi', 'i': 1}
    assert batch.data[1] == {'s': 'hello', 'i': 2}


def test_accept_without_merging(str_dump_node, str_loader_node):
    node = str_dump_node
    node.set_upstream_node('s', str_loader_node)
    asyncio.run(node.accept(batch_id=0))


def test_accept_with_merging(int_str_dump_node, int_loader_node, str_loader_node):
    # single batch
    node = int_str_dump_node
    node.set_upstream_node('i', int_loader_node)
    node.set_upstream_node('s', str_loader_node)
    batch = asyncio.run(node.accept(batch_id=0))
    assert batch.data[0] == {'i': 1, 's': 'hi'}
    assert batch.data[1] == {'i': 2, 's': 'hello'}


def test_accept_with_different_levels(int_str_dump_node, int_loader_node, str_loader_node, capsys):
    def ignore_first(s: str) -> str:
        return s[1:]
    ss_node: TaskNode[str, int] = TaskNode(func=ignore_first)
    ss_node.set_upstream_node('s', str_loader_node)
    int_str_dump_node.set_upstream_node('i', int_loader_node)
    int_str_dump_node.set_upstream_node('s', ss_node)
    asyncio.run(int_str_dump_node.run_and_dump(batch_id=0))
    out, _ = capsys.readouterr()
    assert out == '1 i\n2 ello\n'


def test_get_arg_types():
    def print_str(s: str) -> None:
        pass

    cons: DumpNode[str] = DumpNode(func=print_str)
    assert cons.get_arg_type () == str
