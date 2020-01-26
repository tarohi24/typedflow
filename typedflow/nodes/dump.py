from __future__ import annotations
from dataclasses import dataclass
import logging
import gc
from typing import (
    Any,
    Dict,
)

from typedflow.batch import Batch
from typedflow.exceptions import EndOfBatch, FaultItem

from . import ConsumerNode


__all__ = ['DumpNode', ]
logger = logging.getLogger(__file__)


@dataclass
class DumpNode(ConsumerNode):
    finished: bool = False

    def run_and_dump(self,
                     batch_id: int) -> None:
        if not self.finished:
            try:
                batch: Batch[Dict[str, Any]] = self.accept(batch_id=batch_id)
                self.dump(batch)
                gc.collect()
            except EndOfBatch:
                self.finished: bool = True
        else:
            return

    def dump(self, batch: Batch) -> None:
        for item in batch.data:
            if isinstance(item, FaultItem):
                continue
            elif any([isinstance(val, FaultItem) for val in item.values()]):
                continue
            else:
                self.func(**item)
