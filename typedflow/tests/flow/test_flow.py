"""
Integrated test
int ----->  len(str) + int ---> print
str --/                    ---> save_to_file
                      path --/
"""
import asyncio
from pathlib import Path
import tempfile
from typing import Generator, List

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
        while True:
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
    asyncio.run(fl.async_run())
    fl.run()


def test_type_check():
    loader = str_loader_node()
    dumper = print_dump()
    dumper.set_upstream_node('s', loader)
    flow = Flow(dump_nodes=[dumper, ])
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
