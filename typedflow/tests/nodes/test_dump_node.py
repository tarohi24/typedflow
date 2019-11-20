from typing import TypedDict

import pytest

from typedflow.batch import Batch
from typedflow.nodes import DumpNode


class PrintableArg(TypedDict):
    s: str
    i: int

    def __str__(self):
        return f'{self.s} {str(self.i)}'


@pytest.fixture
def dump_node():

    def func(batch):
        for arg in batch.data:
            print(str(arg))

    node = DumpNode(arg_type=Batch[str], func=func)
    return node


def test_print_dump(dump_node, capsys):
    data = [PrintableArg(s='hi', i=i) for i in range(3)]
    batch = Batch(data=data, batch_id=1)
    dump_node.dump(batch)
    out, _ = capsys.readouterr()
    assert out == ('\n'.join([str(a) for a in data]) + '\n')
