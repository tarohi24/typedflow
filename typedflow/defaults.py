"""
preset classes
"""
from typedflow.arguments import TaskArguments


__all__ = ['StringArgument', 'IntArgument', 'FloarArgument']


class StringArgument(TaskArguments):
    value: str


class IntArgument(TaskArguments):
    value: int


class FloarArgument(TaskArguments):
    value: float
