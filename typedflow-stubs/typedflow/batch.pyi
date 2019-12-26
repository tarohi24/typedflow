from typedflow.exceptions import FaultItem
from typedflow.types import T
from typing import Any, List, Union

class Batch:
    batch_id: int
    data: List[Union[T, FaultItem]]
    def __init__(self, batch_id: Any, data: Any) -> None: ...
