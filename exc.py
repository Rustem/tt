class AheadOfMaxOffsetError(Exception):
    pass


class IntervalNotFoundError(Exception):
    pass


class NodeAuthError(Exception):
    pass


class RetryMaxAttemptsError(Exception):
    def __init__(self, max_attempts, reason=None, *args, **kwargs):
        self.max_attempts = max_attempts
        self.reason = reason
        super(RetryMaxAttemptsError, self).__init__(*args, **kwargs)
