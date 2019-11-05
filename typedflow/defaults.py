"""
preset classes
"""
from dataclasses import dataclass

from typedflow.arguments import TaskArguments


__all__ = ['StringArgument', 'IntArgument', 'FloarArgument']


@dataclass
class StrArgument(TaskArguments):
    value: str


@dataclass
class IntArgument(TaskArguments):
    value: int


@dataclass
class FloarArgument(TaskArguments):
    value: float
