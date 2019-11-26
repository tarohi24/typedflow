import asyncio
from typing import Generator, List, TypedDict

import pytest

from typedflow.batch import Batch
from typedflow.nodes import DumpNode, LoaderNode, TaskNode


class IntStr(TypedDict):
    s: str
    i: int


def str_loader_node() -> LoaderNode[str]:
    def load_str() -> Generator[str, None, None]:
        lst: List[str] = ['hi', 'hello', 'konnichiwa']
        for item in lst:
            yield item
    node: LoaderNode[str] = LoaderNode(func=load_str,
                                       batch_size=2)
    return node


def int_loader_node() -> LoaderNode[int]:
    def load_int() -> Generator[int, None, None]:
        lst: int = [1, 2, 3]
        for item in lst:
            yield item
    node: LoaderNode[str] = LoaderNode(func=load_int,
                                       batch_size=2)
    return node


def str_dump_node() -> DumpNode:
    def printer(s: str) -> None:
        print(s)
    node: DumpNode = DumpNode(func=printer)
    return node


def int_str_dump_node() -> DumpNode:
    def printer(i: int, s: str) -> None:
        print(f'{str(i)} {s}')
    node: DumpNode = DumpNode(func=printer)
    return node


def test_init():
    node = str_dump_node()
    assert len(node.precs) == 0


def test_set_upstream():
    sl = str_loader_node()
    il = int_loader_node()
    node = str_dump_node()
    node.set_upstream_node('loader', sl)
    with pytest.raises(AssertionError):
        node.set_upstream_node('loader', sl)
    assert len(node.precs) == 1
    assert sl._succ_count == 1

    node.set_upstream_node('ld', il)
    assert len(node.precs) == 2
    assert il._succ_count == 1
    assert sl._succ_count == 1


def test_batch_len_and_id():
    node = str_dump_node()
    batches = [
        Batch(batch_id=0, data=[1, 2]),
        Batch(batch_id=0, data=[2, 3]),
    ]
    assert node._get_batch_id(batches) == 0
    assert node._get_batch_len(batches) == 2
    batches.append(Batch(batch_id=1, data=[]))
    with pytest.raises(AssertionError):
        node._get_batch_id(batches)


def test_merging():
    node = int_str_dump_node()
    batch_str = Batch(batch_id=0, data=['hi', 'hello'])
    batch_int = Batch(batch_id=0, data=[1, 2])
    materials = {'s': batch_str, 'i': batch_int}
    batch = node._merge_batches(materials)
    assert batch.data[0] == {'s': 'hi', 'i': 1}
    assert batch.data[1] == {'s': 'hello', 'i': 2}


def test_accept_without_merging():
    node = str_dump_node()
    sl = str_loader_node()
    node.set_upstream_node('s', sl)
    asyncio.run(node.accept(batch_id=0))


def test_accept_with_merging():
    # single batch
    node = int_str_dump_node()
    (node < int_loader_node())('i')
    (node < str_loader_node())('s')
    batch = asyncio.run(node.accept(batch_id=0))
    assert batch.data[0] == {'i': 1, 's': 'hi'}
    assert batch.data[1] == {'i': 2, 's': 'hello'}


def test_accept_with_different_levels(capsys):
    def ignore_first(s: str) -> str:
        return s[1:]
    ss_node: TaskNode[int] = TaskNode(func=ignore_first)
    (ss_node < str_loader_node())('s')

    dumper = int_str_dump_node()
    (dumper < int_loader_node())('i')
    (dumper < ss_node)('s')
    asyncio.run(dumper.run_and_dump(batch_id=0))
    out, _ = capsys.readouterr()
    assert out == '1 i\n2 ello\n'


def test_get_arg_types():
    def print_str(s: str) -> None:
        pass

    cons: DumpNode[str] = DumpNode(func=print_str)
    assert cons.get_arg_types() == {'s': str}


def test_lt_operation():
    sl = str_loader_node()
    sd = str_dump_node()
    (sd < sl)  # do nothing
    (sd < sl)('loader')
    assert sd.precs['loader'] == sl
    with pytest.raises(AssertionError):
        (sl < sd)


def test_gt_operation():
    sl = str_loader_node()
    sd = str_dump_node()
    (sl > sd)('loader')
    assert sd.precs['loader'] == sl


def test_batch_failrure(capsys):
    def a_load() -> Generator[int, None, None]:
        for i in range(10):
            if i % 4 == 0:
                yield 'oh'  # wrong sample
            else:
                yield i

    def b_load() -> Generator[int, None, None]:
        for i in range(10):
            if i % 3 == 0:
                yield 'oh'  # wrong sample
            else:
                yield i

    def addition(a: int, b: int) -> str:
        return str(a + b)

    a_loader: LoaderNode[int] = LoaderNode(func=a_load, batch_size=5)
    b_loader: LoaderNode[int] = LoaderNode(func=b_load, batch_size=5)
    task: TaskNode[int] = TaskNode(func=addition)
    (task < a_loader)('a')
    (task < b_loader)('b')
    dumper = str_dump_node()
    (dumper < task)('s')
    asyncio.run(dumper.run_and_dump(batch_id=0))
    asyncio.run(dumper.run_and_dump(batch_id=1))
    captured = capsys.readouterr()
    assert captured.out == 'ohoh\n2\n4\n10\n14\n'
