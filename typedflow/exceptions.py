__all__ = ['BatchIsEmpty', 'EndOfBatch', 'FaultItem']


class BatchIsEmpty(Exception):
    pass


class EndOfBatch(Exception):
    pass


class FaultItem:
    def __hash__(self):
        return hash('faultitem')

    def __eq__(self, another):
        if isinstance(another, FaultItem):
            return True
        else:
            return False
