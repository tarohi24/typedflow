from dataclasses import dataclass
from typing import Generic, List, Union

from typedflow.exceptions import FaultItem
from typedflow.types import T


__all__ = ['Batch']


@dataclass
class Batch(Generic[T]):
    batch_id: int
    data: List[Union[T, FaultItem]]
