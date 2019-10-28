from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, List, TypeVar

from dataclasses_json import dataclass_json

from typedflow.settings import logdir


@dataclass
class WorkFlow:
    id: int
    jobs: List[Job]

    def get_logpath(self):
        logdir.joinpath(f'{str(self.id)}')


@dataclass_json
@dataclass
class Message:
    workflow: WorkFlow

    def dump(self) -> Path:
        logpath = self.workflow.get_logpath()

class InitialMessage(Message):
    """
    Every workflow starts with a job that receive this instance
    """
    def load(self) -> Message:
        raise NotImplementedError('Implemented not yet')


T = TypeVar('T', bound=Message)
K = TypeVar('K', bound=Message)
V = TypeVar('V', bound=Message)


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
