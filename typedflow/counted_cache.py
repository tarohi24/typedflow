"""
cache that disappears after n times reading
"""
from dataclasses import dataclass, field
from typing import Dict, Generic, Hashable, TypeVar

from typedflow.types import T, H


__all__ = ['CacheTable', ]


@dataclass
class CacheItem(Generic[T]):
    count: int
    value: T


@dataclass
class CacheTable(Generic[H, T]):
    life: int
    cache_table: Dict[H, CacheItem[T]] = field(default_factory=dict)

    def get(self, key: H) -> T:
        item: CacheItem[T] = self.cache_table[key]
        if item.count == 1:
            del self.cache_table[key]
        elif item.count > 1:
            item.count -= 1
        else:
            raise AssertionError()
        return item.value

    def set(self,
            key: H,
            value: T) -> None:
        self.cache_table[key] = CacheItem(count=self.life, value=value)
