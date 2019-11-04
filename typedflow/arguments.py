from dataclasses import dataclass

from dataclasses_json import dataclass_json


__all__ = ['TaskArguments', ]


@dataclass
@dataclass_json
class TaskArguments:
    pass
