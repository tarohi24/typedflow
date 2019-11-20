"""
Integrated test
int ----->  len(str) + int ---> print
str --/                    ---> save_to_file
                      path --/
"""
import asyncio
from pathlib import Path
import tempfile
from typing import List, TypedDict

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


class IntStr(TypedDict):
    i: int
    s: str


def middle_task() -> TaskNode[IntStr, str]:

    def count_chars(si: IntStr) -> str:
        return f'{si["s"]} {str(si["i"])}'

    node = TaskNode(func=count_chars)
    assert node.cache_table.life == 0
    return node


def path_load_node() -> Path:

    def gen_tmp_file() -> Path:
        while True:
            yield Path(tempfile.mkstemp()[1])

    node = LoaderNode(func=gen_tmp_file,
                      batch_size=2)
    return node


def print_dump() -> DumpNode[str]:
    def printer(s: str) -> None:
        print(s)

    node: DumpNode[str] = DumpNode(func=printer)
    return node


class PathStr(TypedDict):
    p: Path
    s: str


def save_dump() -> DumpNode[str]:
    def saver(ps: PathStr) -> None:
        with open(ps['p'], 'w') as fout:
            fout.write(ps['s'])
    node: DumpNode[str] = DumpNode(func=saver)
    return node


def flow():
    sl = str_loader_node()
    il = int_loader_node()
    mt = middle_task()
    assert mt._succ_count == 0
    assert mt.cache_table.life == 0
    mt.set_upstream_node('s', sl)
    mt.set_upstream_node('i', il)
    assert sl._succ_count == 1
    assert sl.cache_table.life == 1
    assert il._succ_count == 1
    assert il.cache_table.life == 1

    pl = path_load_node()
    sd = save_dump()
    sd.set_upstream_node('p', pl)
    sd.set_upstream_node('s', mt)

    pd = print_dump()
    pd.set_upstream_node('s', mt)
    flow = Flow(dump_nodes=[pd, sd])
    return flow


def test_flow_run():
    fl = flow()
    asyncio.run(fl.run())


def test_type_check():
    loader = str_loader_node()
    dumper = print_dump()
    dumper.set_upstream_node('s', loader)
    flow = Flow(dump_nodes=[dumper, ])
    flow.typecheck()
