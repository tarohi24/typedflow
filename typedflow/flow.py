from __future__ import annotations
import asyncio
from dataclasses import dataclass
import logging
from typing import List, Deque, Dict, Tuple, Type

from typedflow.nodes import ConsumerNode, ProviderNode, DumpNode

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

    async def async_run(self,
                        validate: bool = True) -> None:
        if validate:
            self.validate()
        batch_id: int = 0
        while True:
            for node in self.dump_nodes:
                await node.run_and_dump(batch_id=batch_id)
            if all([node.finished for node in self.dump_nodes]):
                return

    def run(self,
            validate: bool = True) -> None:
        asyncio.run(self.async_run())

    def typecheck(self) -> None:
        """
        Check type consistency with inputs/outputs.
        Return nothing when there are no errors, unless raise AssertionError.
        """
        cands: Deque[Tuple[ConsumerNode, Dict[str, ProviderNode]]] = Deque()
        # initially check all dumps
        for node in self.dump_nodes:
            cands.append(
                (node, {name: ups_node for name, ups_node in node.precs.items()}))
        while True:
            try:
                node, ups_dict = cands.pop()
            except IndexError:
                break
            arg_type: Type = node.get_arg_type()
            if len(node.precs) == 1:
                assert len(ups_dict) == 1
                ups_node: ProviderNode = next(iter(ups_dict.values()))
                assert arg_type == ups_node.get_return_type()
            elif len(node.precs) > 1:
                annotations: Dict[str, Type] = arg_type.__annotations__
                assert len(ups_dict) == len(annotations)
                for key in arg_type.keys():
                    ups_dict[key].get_return_type() == [key]
            else:
                raise AssertionError()
