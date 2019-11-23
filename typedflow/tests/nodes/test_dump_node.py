
import pytest

from typedflow.batch import Batch
from typedflow.exceptions import FaultItem
from typedflow.nodes import DumpNode


@pytest.fixture
def dump_node():

    def func(s: str, i: int) -> None:
        print(f'{s} {str(i)}')

    node = DumpNode(func=func)
    return node


def test_print_dump(dump_node, capsys):
    data = [{'s': 'hi', 'i': i} for i in range(3)]
    batch = Batch(data=data, batch_id=1)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == 'hi 0\nhi 1\nhi 2\n'


def test_print_exceptfor_faultitem(dump_node, capsys):
    data = [{'s': 'hi', 'i': i} for i in range(2)] + [FaultItem()]
    batch = Batch(data=data, batch_id=1)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == 'hi 0\nhi 1\n'


def test_faultitem(dump_node, capsys):
    # if the whole item is fault
    data = [FaultItem(), ]
    batch = Batch(data=data, batch_id=0)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == ''


def test_partly_faultitem(dump_node, capsys):
    data = [{'s': 'hi', 'i': i} for i in range(2)] + [{'s': FaultItem(), 'i': 4}]
    batch = Batch(data=data, batch_id=0)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == 'hi 0\nhi 1\n'
