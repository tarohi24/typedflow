from typing import Hashable, TypedDict, TypeVar


__all__ = ['T', 'K', 'H']

# Any types
T = TypeVar('T')
K = TypeVar('K')

# Hashable
H = TypeVar('H', bound=Hashable)
