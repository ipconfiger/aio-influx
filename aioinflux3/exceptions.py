class LogicException(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)