from typing import Hashable, TypeVar

from typedflow.arguments import TaskArguments


__all__ = ['A', 'B', 'T', 'K', 'H']

# Task arguments
A = TypeVar('A', bound=TaskArguments)
B = TypeVar('B', bound=TaskArguments)

# Any types
T = TypeVar('T')
K = TypeVar('K')

# Hashable
H = TypeVar('H', bound=Hashable)
