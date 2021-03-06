"""
Integrated test
int ----->  len(str) + int ---> print
str --/                    ---> save_to_file path --/
"""
from pathlib import Path
import tempfile
from typing import Generator, List, Iterable

import pytest

from typedflow.flow import Flow
from typedflow.nodes import DumpNode, LoaderNode, TaskNode


def str_loader_node() -> LoaderNode[str]:
    def lst() -> List[str]:
        return ['hi', 'hello', 'konnichiwa']
    node: LoaderNode[str] = LoaderNode(func=lst,
                                       batch_size=2)
    return node


def int_loader_node() -> LoaderNode[int]:
    def lst() -> List[int]:
        return list(range(3))
    node: LoaderNode[int] = LoaderNode(func=lst,
                                       batch_size=2)
    return node


def middle_task() -> TaskNode[str]:

    def count_chars(s: str, i: int) -> str:
        return f'{s} {str(i)}'

    node = TaskNode(func=count_chars)
    assert node.cache_table.life == 0
    return node


def path_load_node() -> LoaderNode[Path]:

    def gen_tmp_file() -> Generator[Path, None, None]:
        for _ in range(3):
            yield Path(tempfile.mkstemp()[1])

    node = LoaderNode(func=gen_tmp_file,
                      batch_size=2)
    return node


def print_dump() -> DumpNode:
    def printer(s: str) -> None:
        print(s)

    node: DumpNode = DumpNode(func=printer)
    return node


def save_dump() -> DumpNode:
    def saver(p: Path, s: str) -> None:
        with open(p, 'w') as fout:
            fout.write(s)
    node: DumpNode = DumpNode(func=saver)
    return node


def flow():
    sl = str_loader_node()
    il = int_loader_node()
    mt = middle_task()
    assert mt._succ_count == 0
    assert mt.cache_table.life == 0
    (mt < sl)('s')
    (mt < il)('i')
    assert sl._succ_count == 1
    assert sl.cache_table.life == 1
    assert il._succ_count == 1
    assert il.cache_table.life == 1

    pl = path_load_node()
    sd = save_dump()
    (sd < pl)('p')
    (sd < mt)('s')

    pd = print_dump()
    (pd < mt)('s')
    flow = Flow(dump_nodes=[pd, sd])
    flow.typecheck()
    return flow


def test_flow_run():
    fl = flow()
    fl.typecheck()
    fl.run()


def test_typecheck_success():
    loader = str_loader_node()
    dumper = print_dump()
    dumper.set_upstream_node('s', loader)
    flow = Flow(dump_nodes=[dumper, ])
    flow.typecheck()


def test_typecheck_failure():
    loader = str_loader_node()
    dumper = print_dump()
    # misdirection
    with pytest.raises(AssertionError):
        (loader < dumper)('s')
    # misspell
    (dumper < loader)('ss')
    flow = Flow([dumper, ])
    with pytest.raises(AssertionError):
        flow.typecheck()
    # invalid types
    loader = int_loader_node()
    dumper = print_dump()
    (dumper < loader)('s')
    flow = Flow([dumper, ])
    with pytest.raises(AssertionError):
        flow.typecheck()


def test_incoming_multiple_node(capsys):
    def op(s: str, i: int) -> int:
        return len(s) + i

    def dump_int(i: int) -> int:
        print(str(i))

    str_loader = str_loader_node()
    int_loader = int_loader_node()
    op_node = TaskNode(op)
    (op_node < str_loader)('s')
    (op_node < int_loader)('i')
    dumper = DumpNode(dump_int)
    (dumper < op_node)('i')
    flow = Flow(dump_nodes=[dumper, ])
    flow.typecheck()
    flow.run()
    captured = capsys.readouterr()
    assert captured.out == '2\n6\n12\n'


def test_arg_inheritance():
    def load() -> List[int]:
        return []

    def task(a: int) -> List[int]:
        pass

    def dump(a: Iterable[int]) -> None:
        pass

    node_load: LoaderNode = LoaderNode(func=load)
    node_task: TaskNode = TaskNode(func=task)
    node_dump: DumpNode = DumpNode(func=dump)
    (node_task < node_load)('a')
    (node_dump < node_task)('a')
    flow = Flow(dump_nodes=[node_dump, ])
    flow.typecheck()


def test_declare_inputs_when_definition():
    def load() -> List[int]:
        return []

    def task(a: int) -> List[int]:
        pass

    def dump(a: Iterable[int]) -> None:
        pass

    node_load = LoaderNode(load)
    node_task = TaskNode(task)({'a': node_load})
    node_dump = DumpNode(dump)({'a': node_task})
    flow = Flow([node_dump, ])
    flow.typecheck()


def test_declare_inputs_when_definition_with_multiple_args():
    def load_int() -> List[int]:
        return []

    def load_str() -> Generator[str, None, None]:
        for i in range(3):
            yield 'a'

    def task(a: int, b: str) -> List[int]:
        pass

    def dump(a: Iterable[int]) -> None:
        pass

    node_load = LoaderNode(load_int)
    node_task = TaskNode(task)({'a': node_load})
    node_dump = DumpNode(dump)({'a': node_task})
    flow = Flow([node_dump, ])
    with pytest.raises(AssertionError):
        flow.typecheck()

    node_load_int = LoaderNode(load_int)
    node_load_str = LoaderNode(load_str)
    node_task = TaskNode(task)({'a': node_load_int, 'b': node_load_str})
    node_dump = DumpNode(dump)({'a': node_task})
    flow = Flow([node_dump, ])
    flow.typecheck()
    assert node_task.cache_table.life == 1


def test_debug_mode():
    def load_str() -> Generator[str, None, None]:
        yield 'a'
        yield 'a'
        yield 3

    def task(a: str) -> List[int]:
        return len(a)

    def dump(a: Iterable[int]) -> None:
        print(str(a))

    node_load = LoaderNode(load_str)
    node_task = TaskNode(task)({'a': node_load})
    node_dump = DumpNode(dump)({'a': node_task})
    flow = Flow([node_dump, ])
    flow.typecheck()
    flow.run()

    def load_str() -> Generator[str, None, None]:
        yield 'a'
        yield 'a'
        yield 3
    node_load = LoaderNode(load_str)
    node_task = TaskNode(task)({'a': node_load})
    node_dump = DumpNode(dump)({'a': node_task})
    flow = Flow([node_dump, ], debug=True)
    flow.typecheck()
    with pytest.raises(TypeError):
        flow.run()
