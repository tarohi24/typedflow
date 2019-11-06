from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import List

from typedflow.nodes import DumpNode

__all__ = ['Flow']


logger = logging.getLogger(__file__)


@dataclass
class Flow:
    dump_nodes: List[DumpNode]

    def validate(self) -> None:
        """
        not implemented because Python Generic doesn't offer
        any ways to access to the actual type
        """
        pass

    async def run(self,
                  validate: bool = True) -> None:
        if validate:
            self.validate()
        batch_id: int = 0
        while True:
            async for node in self.dump_nodes:
                await node.run_and_dump(batch_id=batch_id)
            if all([node.finished for node in self.dump_nodes]):
                return
