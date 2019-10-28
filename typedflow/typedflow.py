from dataclasses import dataclass
from typing import TypeVar, Generic

from dataclasses_json import dataclass_json


@dataclass
class WorkFlow:
    id: int


@dataclass_json
@dataclass
class Message:
    workflow: WorkFlow


T = TypeVar('T', bound=Message)
K = TypeVar('K', bound=Message)


@dataclass
class Job(Generics[T, K]):
    """
    A job which receives an instance of T
    and sends an instace of K
    """
    workflow: WorkFlow

    def process(self,
                message: T) -> K:
        raise NotImplementedError('Implemented not yet')
