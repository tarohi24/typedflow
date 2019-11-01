from typing import List

import pytest

from typedflow.typedflow import (
    Task, Pipeline, DataLoader, Dumper
)
from typedflow.utils import dump_print


@pytest.fixture
def loader() -> DataLoader[str]:
    data: List[str] = [
        'This is a test.',
        'Here we are.',
    ]
    loader: DataLoader[str] = DataLoader(gen=data)
    return loader


@pytest.fixture
def count_char_tasks() -> List[Task]:
    count_word: Task[str, int] = Task[str, int](lambda s: len(s))
    convert_to_str: Task[int, str] = Task[int, str](lambda x: str(x))
    return [count_word, convert_to_str]


@pytest.fixture
def dumper() -> Dumper[str]:
    dumper: Dumper[str] = Dumper[str](dump_print)
    return dumper


@pytest.fixture
def pl(loader, count_char_tasks, dumper):
    pipeline: Pipeline = Pipeline(
        loader, count_char_tasks, dumper)
    return pipeline


@pytest.fixture
def invalid_pl(count_char_tasks, dumper):
    data: List = [
        'This is a test.',
        'Here we are.',
        1
    ]
    loader: DataLoader[str] = DataLoader(gen=data)
    pipeline: Pipeline = Pipeline(
        loader, count_char_tasks, dumper)
    return pipeline


def test_process(pl, capsys):
    pl.run()
    out, _ = capsys.readouterr()
    assert out == '15\n12\n'


def test_except_batch(invalid_pl, capsys):
    invalid_pl.run()
    out, _ = capsys.readouterr()
    assert out == ''
