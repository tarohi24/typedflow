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

from typedflow.batch import Batch
from typedflow.flow import Flow
from typedflow.nodes import DumpNode, LoaderNode, TaskNode


class IntStr(TypedDict):
    i: int
    s: str


def str_loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    node: LoaderNode[str] = LoaderNode(orig=lst, batch_size=2)
    return node


def int_loader_node() -> LoaderNode[int]:
    lst = list(range(3))
    node: LoaderNode[int] = LoaderNode(orig=lst)
    return node


def middle_task() -> TaskNode[IntStr, str]:

    def count_chars(si: IntStr) -> str:
        return f'{si["s"]} {str(si["i"])}'

    sl = str_loader_node()
    il = int_loader_node()
    node = TaskNode(func=count_chars, arg_type=IntStr)
    assert node.cache_table.life == 0
    node.set_upstream_node('s', sl)
    node.set_upstream_node('i', il)
    return node


def path_load_node() -> Path:

    def gen_tmp_file():
        while True:
            yield Path(tempfile.mkstemp()[1])

    gen = gen_tmp_file()
    node = LoaderNode(orig=gen, batch_size=2)
    return node


def print_dump() -> DumpNode[str]:
    def printer(batch: Batch[str]) -> None:
        for item in batch.data:
            return

    mt = middle_task()
    node: DumpNode[str] = DumpNode(arg_type=str, func=printer)
    node.set_upstream_node('_', mt)
    return node


class PathedStr(TypedDict):
    s: str
    p: Path


def save_dump() -> DumpNode[str]:
    def saver(batch: Batch[PathedStr]) -> None:
        for item in batch.data:
            with open(item['p'], 'w') as fout:
                fout.write(item['s'])
    mt = middle_task()
    pl = path_load_node()
    assert mt._succ_count == 0
    assert mt.cache_table.life == 0
    node: DumpNode[str] = DumpNode(arg_type=PathedStr, func=saver)
    node.set_upstream_node('s', mt)
    node.set_upstream_node('p', pl)
    assert pl.cache_table.life == 1
    assert mt._succ_count == 1
    assert mt.cache_table.life == 1
    return node


def flow():
    pd = print_dump()
    sd = save_dump()
    flow = Flow(dump_nodes=[pd, sd])
    return flow


def test_flow_run():
    fl = flow()
    asyncio.run(fl.run())


def test_type_check():
    fl = flow()
    fl.typecheck()
