"""
Utilities class
"""
from pathlib import Path
from typing import Callable, List

from typedflow.typedflow import Batch, T


def dump_to_each_file(batch: Batch[T],
                      path_method: Callable[[int], Path]) -> None:
    """
    Dump the batch in a file

    Parameters
    -----
    batch
        batch whose items are serializable (i.e. they have `to_json()`)
    path_method
        A function which transforms batch_id into a file path
        where the batch is saved.
    """
    path: Path = path_method(batch.batch_id)
    data: List[str] = [item.to_json() for item in batch.data]
    with open(path, 'w') as fout:
        for js in data:
            fout.write(js + '\n')


def dump_to_one_file(batch: Batch[T],
                     path: Path) -> None:
    """
    Dump the batch in a file. Unlike `dump_to_each_file`,
    this dumps all the batch in the identical file
    """
    data: List[str] = [item.to_json() for item in batch.data]
    with open(path, 'a') as fout:
        for js in data:
            fout.write(js + '\n')


def dump_print(batch: Batch[T]) -> None:
    for item in batch.date:
        print(item)
