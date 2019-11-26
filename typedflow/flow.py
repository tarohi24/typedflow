from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import Deque, Dict, List, Tuple, Type, Union, Set

from typedflow.nodes import ConsumerNode, ProviderNode, DumpNode, LoaderNode

__all__ = ['Flow']


logger = logging.getLogger(__file__)
Node = Union[ProviderNode, ConsumerNode]


@dataclass
class Flow:
    dump_nodes: List[DumpNode]

    def validate(self) -> None:
        """
        not implemented because Python Generic doesn't offer
        any ways to access to the actual type
        """

    def get_loader_nodes(self) -> List[LoaderNode]:
        loaders: List[LoaderNode] = []
        visited: List[Node] = []
        cands: Deque[Node] = Deque()
        cands.extend(self.dump_nodes)
        while True:
            try:
                node: Node = cands.pop()
            except IndexError:
                return loaders
            if node in visited:
                continue
            else:
                visited.append(node)

            if isinstance(node, LoaderNode):
                if node not in loaders:
                    loaders.append(node)
            else:
                cands.extend([n for n in node.precs.values()
                              if n not in visited])

    def run(self,
            validate: bool = True) -> None:
        if validate:
            self.validate()
        batch_id: int = 0
        while True:
            for node in self.dump_nodes:
                node.run_and_dump(batch_id=batch_id)
            if all([node.finished for node in self.dump_nodes]):
                return

    def typecheck(self) -> None:
        """
        Check type consistency with inputs/outputs.
        Return nothing when there are no errors, unless raise AssertionError.
        """
        cands: Deque[Tuple[ConsumerNode, Dict[str, ProviderNode]]] = Deque()
        # initially check all dumps
        for node in self.dump_nodes:
            cands.append(
                (node, {name: ups_node.get_return_type()
                        for name, ups_node in node.precs.items()}))
        while True:
            try:
                node, ups_dict = cands.pop()
            except IndexError:
                break
            arg_types: Type = node.get_arg_types()
            keys: Set[str] = set(arg_types.keys())
            assert keys == set(ups_dict.keys()), f'Invalid arguments. Expected: {arg_types}, Actual: {ups_dict}'
            for key in keys:
                if ups_dict[key] != arg_types[key]:
                    raise AssertionError(f'Invalid type for arg {key}: Expected {arg_types[key]}, Actual {ups_dict[key]}')
            for new_node in node.precs.values():
                if isinstance(new_node, ConsumerNode):
                    cands.append(
                        (new_node, {name: ups_node.get_return_type()
                                    for name, ups_node in new_node.precs.items()}))

        # check batch_size
        loaders: List[LoaderNode] = self.get_loader_nodes()
        assert len({ld.batch_size for ld in loaders}) == 1
