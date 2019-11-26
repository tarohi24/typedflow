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


def tasknode() -> TaskNode[int]:
    node: TaskNode[int] = TaskNode(func=count_chars)
    node.set_upstream_node('s', str_loader_node())
    return node


def test_init():
    node = tasknode()
    assert len(node.precs) == 1
    assert node._succ_count == 0


def test_provide():
    node = tasknode()
    batch = node.get_or_produce_batch(batch_id=0)
    assert batch.data == [2, 5]
    batch = node.get_or_produce_batch(batch_id=1)  # noqa
    assert batch.data == [len('konnichiwa')]


def test_fault_item():
    loader: LoaderNode[str] = LoaderNode(func=lst_with_fi, batch_size=2)
    node: TaskNode[int] = TaskNode(func=count_chars)
    (node < loader)('s')
    batch = node.get_or_produce_batch(batch_id=0)
    assert batch.data == [2, 5]
    batch = node.get_or_produce_batch(batch_id=1)  # noqa
    assert batch.data[1] == len('konnichiwa')


def test_lt_and_gt():
    loader: LoaderNode[str] = LoaderNode(func=lst_with_fi, batch_size=2)
    node = tasknode()
    dumper: DumpNode = DumpNode(func=dump_int)
    (node < loader)('orig')
    (node > dumper)('conv')


def test_with_fault_item():

    def load() -> List[int]:
        return list(range(10))

    def a_task(a: int) -> int:
        if a % 3 != 0:
            return a + 1
        else:
            raise Exception()

    def b_task(b: int) -> int:
        if b % 4 != 0:
            return b
        else:
            raise Exception()

    def c_task(a: int, b: int) -> str:
        return str(a + b)

    loader: LoaderNode[int] = LoaderNode(func=load, batch_size=5)
    a: TaskNode[int] = TaskNode(func=a_task)
    b: TaskNode[int] = TaskNode(func=b_task)
    c: TaskNode[int] = TaskNode(func=c_task)
    (a < loader)('a')
    (b < loader)('b')
    (c < a)('a')
    (c < b)('b')
    assert a.get_or_produce_batch(batch_id=0).data == [FaultItem(), 2, 3, FaultItem(), 5]
    assert b.get_or_produce_batch(batch_id=0).data == [FaultItem(), 1, 2, 3, FaultItem()]
    loader: LoaderNode[int] = LoaderNode(func=load, batch_size=5)
    a: TaskNode[int] = TaskNode(func=a_task)
    b: TaskNode[int] = TaskNode(func=b_task)
    c: TaskNode[int] = TaskNode(func=c_task)
    (a < loader)('a')
    (b < loader)('b')
    (c < a)('a')
    (c < b)('b')
    res = c.get_or_produce_batch(batch_id=0)
    assert len(res.data) == 5
    assert res.data == [FaultItem(), '3', '5', FaultItem(), FaultItem()]
