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
from typedflow.tasks import Dumper, DataLoader, Task
from typedflow.nodes import DumpNode, LoaderNode, TaskNode


class IntStr(TypedDict):
    i: int
    s: str


def str_loader_node() -> LoaderNode[str]:
    lst: List[str] = ['hi', 'hello', 'konnichiwa']
    loader: DataLoader[str] = DataLoader(gen=lst, batch_size=2)
    node: LoaderNode[str] = LoaderNode(loader=loader)
    return node


def int_loader_node() -> LoaderNode[int]:
    lst: int = [1, 2, 3]
    loader: DataLoader[int] = DataLoader(gen=lst, batch_size=2)
    node: LoaderNode[int] = LoaderNode(loader=loader)
    return node


def middle_task() -> TaskNode[IntStr, str]:

    def count_chars(si: IntStr) -> str:
        return f'{si["s"]} {str(si["i"])}'

    sl = str_loader_node()
    il = int_loader_node()
    task = Task(func=count_chars)
    node = TaskNode(task=task, arg_type=IntStr)
    assert node.cache_table.life == 0
    node.set_upstream_node('s', sl)
    node.set_upstream_node('i', il)
    return node


def path_load_node() -> Path:

    def gen_tmp_file():
        while True:
            yield Path(tempfile.mkstemp()[1])

    gen = gen_tmp_file()
    loader = DataLoader(gen=gen, batch_size=2)
    node = LoaderNode(loader=loader)
    return node


def print_dump() -> DumpNode[str]:
    def printer(batch: Batch[str]) -> None:
        for item in batch.data:
            return

    mt = middle_task()
    dumper: Dumper[str] = Dumper(func=printer)
    node: DumpNode[str] = DumpNode(arg_type=str, dumper=dumper)
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
    dumper: Dumper[str] = Dumper(func=saver)
    node: DumpNode[str] = DumpNode(arg_type=PathedStr, dumper=dumper)
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
