from typing import TypedDict

import pytest

from typedflow.batch import Batch
from typedflow.exceptions import FaultItem
from typedflow.nodes import DumpNode


class PrintableArg(TypedDict):
    s: str
    i: int


@pytest.fixture
def dump_node():

    def func(si: PrintableArg) -> None:
        print(f'{si["s"]} {str(si["i"])}')

    node = DumpNode(func=func)
    return node


def test_print_dump(dump_node, capsys):
    data = [PrintableArg(s='hi', i=i) for i in range(3)]
    batch = Batch(data=data, batch_id=1)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == 'hi 0\nhi 1\nhi 2\n'


def test_print_exceptfor_faultitem(dump_node, capsys):
    data = [PrintableArg(s='hi', i=i) for i in range(2)] + [FaultItem()]
    batch = Batch(data=data, batch_id=1)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == 'hi 0\nhi 1\n'
