from dataclasses import dataclass

from typedflow.arguments import TaskArguments


@dataclass
class Arg(TaskArguments):
    string: str
    int_value: int
