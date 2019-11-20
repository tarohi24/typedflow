from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import get_type_hints, Callable, List, Deque, Tuple, Type

from typedflow.nodes import ConsumerNode, DumpNode

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
            for node in self.dump_nodes:
                await node.run_and_dump(batch_id=batch_id)
            if all([node.finished for node in self.dump_nodes]):
                return


    def typecheck(self) -> None:
        """
        Raises exceptions only if type check fails
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
            args: Dict[str, Type] = node.get_arg_types()
            prov_return_types: Dict[str, Type] = {name: node.get_return_type()
                                                  for name, node in ups_dict.items()}
            assert all(args[key] == prov_return_types[key] for key in args.keys())
