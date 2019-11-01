from typing import List

import pytest

from typedflow.typedflow import (
    Task, Pipeline, DataLoader, Dumper
)
from typedflow.utils import dump_print


@pytest.fixture
def pl():
    data: List[str] = [
        'This is a test.',
        'Here we are.',
    ]
    loader: DataLoader[str] = DataLoader(gen=data)
    count_word: Task[str, int] = Task[str, int](lambda s: len(s))
    convert_to_str: Task[int, str] = Task[int, str](lambda x: str(x))
    dumper: Dumper[str] = Dumper[str](dump_print)
    pipeline: Pipeline = Pipeline(
        loader, [count_word, convert_to_str, ], dumper)
    return pipeline


def test_process(pl, capsys):
    pl.run()
    out, _ = capsys.readouterr()
    assert out == '15\n12\n'
