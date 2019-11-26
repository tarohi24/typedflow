from __future__ import annotations
from dataclasses import dataclass, field
import logging
from typing import (
    get_args,
    get_type_hints,
    overload,
    Any,
    Callable,
    Dict,
    Iterator,
    Iterable,
    Generic,
    Generator,
    List,
    Union,
    Type,
    TypedDict,
    TypeVar
)

from typedflow.batch import Batch
from typedflow.counted_cache import CacheTable
from typedflow.exceptions import EndOfBatch, FaultItem
from typedflow.types import K

__all__ = ['LoaderNode', 'TaskNode', 'DumpNode']


logger = logging.getLogger(__file__)
TD = TypeVar('TD', bound=TypedDict)


@dataclass
class ProviderNode(Generic[K]):
    """
    LoaderNode or TaskNode
    """
    func: Callable[..., K]  # not a generator in order to get annotatinos
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


@dataclass
class ConsumerNode:
    """
    TaskNode or DumpNode
    Note: this is not defined as a dataclass due to the inheritance problem
    see: https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses  # noqa
    """
    func: Callable[..., Any]
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


@dataclass
class LoaderNode(ProviderNode[K]):
    """
    caution: We don't offer any interfaces to accept generator.
    We only accept generative function because we cannot obtain any
    typing information from a genarator.
    This behavior may change if Python supports getting typings from
    a generator
    """
    batch_size: int = 16
    itr: Iterator[K] = field(init=False)

    def __post_init__(self):
        ProviderNode.__post_init__(self)
        self.itr: Iterator[K] = iter(self.func())

    def get_return_type(self) -> Type[K]:
        typ: Type[Iterable[K]] = get_type_hints(self.func)['return']
        try:
            return get_args(typ)[0]
        except IndexError:
            raise AssertionError(f'function {self.func.__name__} may not return iterbale')

    def load(self) -> Generator[Batch[K], None, None]:
        lst: List[K] = []
        batch_id: int = 0
        while True:
            for _ in range(self.batch_size):
                try:
                    item: K = next(self.itr)
                except StopIteration:
                    batch: Batch[K] = Batch[K](batch_id=batch_id, data=lst)
                    if len(batch.data) > 0:
                        yield batch
                    return
                lst.append(item)
            batch: Batch[K] = Batch[K](batch_id=batch_id, data=lst)
            yield batch
            batch_id += 1
            lst: List[K] = []  # noqa

    def get_or_produce_batch(self,
                             batch_id: int) -> Batch[K]:
        """
        CAUTION: for unit testing of this method, we have to set
        cache_table.life = 1 in advance (defualt 0)
        """
        try:
            return self.cache_table.get(batch_id)
        except KeyError:
            try:
                batch: Batch[K] = next(self.load())
            except StopIteration:
                raise EndOfBatch()
            self.cache_table.set(key=batch_id, value=batch)
            return self.cache_table.get(batch_id)


@dataclass(init=False)
class TaskNode(ConsumerNode, ProviderNode[K]):
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


@dataclass
class DumpNode(ConsumerNode):
    finished: bool = False

    def run_and_dump(self,
                           batch_id: int) -> None:
        if not self.finished:
            try:
                batch: Batch[Dict[str, Any]] = self.accept(batch_id=batch_id)
                self.dump(batch)
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
