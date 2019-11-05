from dataclasses import dataclass
from typing import Generic, List, Union

from typedflow.exceptions import FaultItem
from typedflow.types import A


__all__ = ['Batch']


@dataclass
class Batch(Generic[A]):
    """
    Every batch consists of instances of TaskArguments
    """
    batch_id: int
    data: List[Union[A, FaultItem]]
