from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import (
    get_type_hints,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Union,
    Type,
)
import traceback

from typedflow.batch import Batch
from typedflow.exceptions import EndOfBatch, FaultItem
from typedflow.types import K

from . import ConsumerNode, ProviderNode


__all__ = ['TaskNode', ]
logger = logging.getLogger(__file__)


@dataclass(init=False)
class TaskNode(ConsumerNode, ProviderNode[K], Callable):
    """
    This is not a dataclass because it dataclass doesn't work
    if it is inherited from multiple super classes
    """

    def __init__(self,
                 func: Callable[..., K]):
        ConsumerNode.__init__(self, func=func)
        ConsumerNode.__post_init__(self)
        ProviderNode.__init__(self, func=func)
        ProviderNode.__post_init__(self)

    def get_return_type(self) -> Type[K]:
        typ: Type[Iterable[K]] = get_type_hints(self.func)['return']
        return typ

    def process(self,
                batch: Batch[Union[Dict[str, Any]], FaultItem]) -> Batch[K]:
        if len(batch.data) == 0:
            raise EndOfBatch()
        products: List[Union[FaultItem, Dict[str, Any]]] = []
        for item in batch.data:
            if isinstance(item, FaultItem):
                products.append(FaultItem())
                continue
            try:
                products.append(self.func(**item))
            except Exception as e:
                logger.warn(repr(e))
                traceback.print_tb(e.__traceback__)
                products.append(FaultItem())
        return Batch[K](batch_id=batch.batch_id,
                        data=products)

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[K]:
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            arg: Batch[Dict[str, Any]] = self.accept(batch_id=batch_id)
            product: Batch[K] = self.process(arg)
            self.cache_table.set(key=batch_id, value=product)
            return self.cache_table.get(batch_id)

    def __lt__(self,
               another: ProviderNode) -> Callable[[str], None]:
        """
        Do not implement using super() (it has problems
        when a class inherits from multiple super classes)
        """
        return self.lt_op(another=another)

    def __gt__(self,
               another: ConsumerNode) -> Callable[[str], None]:
        return self.gt_op(another=another)
