from __future__ import annotations
from dataclasses import dataclass, field
import logging
from typing import (
    get_type_hints,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Type,
    TypeVar
)
from typedflow.batch import Batch
from typedflow.counted_cache import CacheTable
from typedflow.types import K


__all__ = ['ConsumerNode', 'ProviderNode']
logger = logging.getLogger(__file__)
T = TypeVar('T')


@dataclass
class ConsumerNode:
    """
    TaskNode or DumpNode
    Note: this is not defined as a dataclass due to the inheritance problem
    see: https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses  # noqa
    """
    func: Callable[..., Any]
    debug: bool = False
    precs: Dict[str, ProviderNode] = field(init=False)

    def __post_init__(self):
        self.precs: Dict[str, ProviderNode] = dict()

    def set_upstream_node(self,
                          key: str,
                          node: ProviderNode) -> None:
        assert key not in self.precs
        self.precs[key] = node
        node.add_succ()

    def get_arg_types(self) -> Dict[str, Type]:
        args: Dict[str, Type] = {key: typ
                                 for key, typ
                                 in get_type_hints(self.func).items()
                                 if key != 'return'}
        return args

    @staticmethod
    def _get_batch_id(batches: List[Batch]) -> int:
        batch_id: int = batches[0].batch_id
        assert all([batch.batch_id == batch_id for batch in batches])
        return batch_id

    @staticmethod
    def _get_batch_len(batches: List[Batch]) -> int:
        batch_len: int = min([len(b.data) for b in batches])
        if any([len(batch.data) != batch_len for batch in batches]):
            logger.warn('Different length among batches')
            logger.warn([len(batch.data) for batch in batches])
        return batch_len

    def _merge_batches(self,
                       materials: Dict[str, Batch]) -> Batch[Dict[str, Any]]:
        """
        return kwargs (without any type checks)
        """
        mat_batches: List[Batch] = list(materials.values())
        batch_len: int = self._get_batch_len(mat_batches)
        keys: List[str] = list(materials.keys())
        data: List[Dict[str, Any]] = list()
        for i in range(batch_len):
            data.append({key: materials[key].data[i] for key in keys})
        batch_id: int = self._get_batch_id(mat_batches)
        batch = Batch(batch_id=batch_id, data=data)
        return batch

    def accept(self,
               batch_id: int) -> Batch[Dict[str, Any]]:
        """
        merge all the arguments items into an instance of T (=arg_type)
        """
        materials: Dict[str, Batch] = dict()
        for key, prec in self.precs.items():
            materials[key] = prec.get_or_produce_batch(batch_id=batch_id)
        # check lengths of batches
        merged_batch: Batch[Dict[str, Any]] = self._merge_batches(materials=materials)
        return merged_batch

    def lt_op(self,
              another: ProviderNode) -> Callable[[str], None]:
        assert isinstance(another, ProviderNode), 'In a < b, b should be an ProviderNode instance'
        func: Callable[[str], None] = lambda s: self.set_upstream_node(s, another)
        return func

    def __lt__(self,
               another: ProviderNode) -> Callable[[str], None]:
        return self.lt_op(another=another)

    def __gt__(self, another):
        raise AssertionError('ConsumerNode does not support > operation')

    def __call__(self: T,
                 args: Dict[str, ProviderNode]) -> T:
        """
        This method is to define argument origins. This allows to declare both
        function and its inputs at the same time.

        Example:
        >>> TaskNode(func)({'a': node_load, 'b': node_task_b})
        None
        """
        assert len(self.precs) == 0, 'Some arguments have been already set'
        for name, prec in args.items():
            self.set_upstream_node(name, prec)
        return self


@dataclass
class ProviderNode(Generic[K]):
    """
    LoaderNode or TaskNode
    """
    func: Callable[..., K]  # not a generator in order to get annotatinos
    debug: bool = False
    cache_table: CacheTable[int, Batch[K]] = field(init=False)
    _succ_count: int = field(init=False)

    def __post_init__(self):
        self._succ_count: int = 0
        self.cache_table: CacheTable[int, Batch[K]]\
            = CacheTable[int, Batch[K]](life=0)

    def get_return_type(self) -> Type[K]:
        ...

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[K]:
        ...

    def add_succ(self):
        self._succ_count += 1
        self.cache_table.life += 1

    def gt_op(self,
              another: ConsumerNode) -> Callable[[str], None]:
        """
        This is not implemented inside __gt__ due to the inhetitance problems
        """
        assert isinstance(another, ConsumerNode)
        return another < self

    def __gt__(self,
               another: ConsumerNode) -> Callable[[str], None]:
        return self.gt_op(another=another)

    def __lt__(self, another):
        raise AssertionError('ConsumerNode does not support > operation')
