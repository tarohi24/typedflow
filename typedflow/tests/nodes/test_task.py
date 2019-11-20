import asyncio
from typing import List, Union

from typedflow.exceptions import FaultItem
from typedflow.nodes import TaskNode, LoaderNode, DumpNode


def lst() -> List[str]:
    return ['hi', 'hello', 'konnichiwa']


def count_chars(s: str) -> int:
    return len(s)


def lst_with_fi() -> List[Union[str, FaultItem]]:
    return ['hi', 'hello'] + [FaultItem()] + ['konnichiwa']


def str_loader_node() -> LoaderNode[str]:
    node: LoaderNode[str] = LoaderNode(func=lst,
                                       batch_size=2)
    return node


def dump_int(i: int) -> None:
    i + 1   # do nothing


def tasknode() -> TaskNode[str, int]:
    node: TaskNode[str, int] = TaskNode(func=count_chars)
    node.set_upstream_node('s', str_loader_node())
    return node


def test_init():
    node = tasknode()
    assert len(node.precs) == 1
    assert node._succ_count == 0


def test_provide():
    node = tasknode()
    batch = asyncio.run(node.get_or_produce_batch(batch_id=0))
    assert batch.data == [2, 5]
    batch = asyncio.run(node.get_or_produce_batch(batch_id=1))  # noqa
    assert batch.data == [len('konnichiwa')]


def test_fault_item():
    loader: LoaderNode[str] = LoaderNode(func=lst_with_fi, batch_size=2)
    node: TaskNode[str, int] = TaskNode(func=count_chars)
    (node < loader)('loader')
    batch = asyncio.run(node.get_or_produce_batch(batch_id=0))
    assert batch.data == [2, 5]
    batch = asyncio.run(node.get_or_produce_batch(batch_id=1))  # noqa
    assert batch.data[1] == len('konnichiwa')


def test_lt_and_gt():
    loader: LoaderNode[str] = LoaderNode(func=lst_with_fi, batch_size=2)
    node = tasknode()
    dumper: DumpNode[int] = DumpNode(func=dump_int)
    (node < loader)('orig')
    (node > dumper)('conv')
