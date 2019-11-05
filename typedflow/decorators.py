from typing import Callable

from typedflow.defaults import StringArgument, IntArgument, FloarArgument


def accept_str(func: Callable[[str], ...]):
    def wrapper(sa: StringArgument):
        s: str = StringArgument.value
        return func(s)
    def wrapper
