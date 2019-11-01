from pathlib import Path
import shutil
import tempfile
from typing import List
import unittest

from typedflow.typedflow import DataLoader, Task, Dumper, Pipeline
from typedflow.utils import dump_to_one_file


class TestDump(unittest.TestCase):
    def setUp(self):
        self.test_dir: Path = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_dump(self):
        data: List[str] = [
            'This is a test.',
            'Here we are.',
        ]
        loader: DataLoader[str] = DataLoader(gen=data)
        count_word: Task[str, int] = Task[str, int](lambda s: len(s))
        convert_to_str: Task[int, str] = Task[int, str](lambda x: str(x))
        dumper: Dumper[str] = Dumper(
            lambda b: dump_to_one_file(
                b, self.test_dir.joinpath('thi')))
        pipeline: Pipeline = Pipeline(
            loader, [count_word, convert_to_str, ], dumper)
        pipeline.run()
