__author__ = 'vidma'


class TimeLimitExceeded(Exception):
    """
    exception to indicate a time limit exceeded
    """
    def __init__(self, *args, **kwargs):
        super(TimeLimitExceeded, self).__init__(*args, **kwargs)

