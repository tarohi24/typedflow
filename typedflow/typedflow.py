from dataclasses import dataclass
from typing import Generic, TypeVar

from dataclasses_json import dataclass_json

from typedflow.settings import logdir


@dataclass
class WorkFlow:
    id: int

    def get_logpath(self):
        logdir.joinpath(f'{str(self.id)}')


@dataclass_json
@dataclass
class Message:
    workflow: WorkFlow


T = TypeVar('T', bound=Message)
K = TypeVar('K', bound=Message)


@dataclass
class Job(Generic[T, K]):
    """
    A job which receives an instance of T
    and sends an instace of K
    """
    workflow: WorkFlow

    def process(self,
                message: T) -> K:
        raise NotImplementedError('Implemented not yet')
