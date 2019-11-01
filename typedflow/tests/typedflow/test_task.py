from typing import List
import unittest

from typedflow.typedflow import (
    Task, Pipeline, DataLoader, Dumper
)


class TaskTest(unittest.TestCase):

    def setUp(self):
        data: List[str] = [
            'This is a test.',
            'Here we are.',
        ]
        loader: DataLoader[str] = DataLoader(gen=data)
        count_word: Task[str, int] = Task[str, int](lambda s: len(s))
        convert_to_str: Task[int, str] = Task[int, str](lambda x: str(x))
        dumper: Dumper[str] = Dumper[str](print)
        self.pipeline: Pipeline = Pipeline(
            loader, [count_word, convert_to_str, ], dumper)

    def test_process(self):
        self.pipeline.run()
