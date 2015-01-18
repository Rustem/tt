import gevent
from exc import RetryMaxAttemptsError

RETRY_RESET, RETRY_BREAK, RETRY_CONTINUE = range(3)

default_options = {
    'tag': '',
    'backoff': 1,  # default retry backoff interval
    'max_backoff': 20,  # default max retry backoff interval
    'constant_factor': 1.5,  # default backoff multiplier
    'max_attempts': 20,  # default max of attempts
}

retry_jitter = 0.15


""""
RetryWithBackoff implements retry with exponential backoff using
the supplied options as parameters. When fn returns RetryContinue
and the number of retry attempts haven't been exhausted, fn is
retried. When fn returns RetryBreak, retry ends. As a special case,
if fn returns RetryReset, the backoff and retry count are reset to
starting values and the next retry occurs immediately. Returns an
err or result if the maximum number of retries is exceeded or if the fn
returns an RetryBreak
"""


def update_opts(opts):
    for k, v in default_options.iteritems():
        if k in opts:
            continue
        opts[k] = v


def RetryWithBackoff(opts, fn, args=None, kwargs=None):
    """`fn` function must follow the interface suggested:
        * it should return tuple <status, err> where
            status - backoff status
            err - error that happend in function to propogate it to caller."""
    args = args or ()
    kwargs = kwargs or {}
    update_opts(opts)
    count = 0
    backoff = opts['backoff']
    while True:
        count += 1
        status, err_or_rv = fn(*args, **kwargs)
        print status, err_or_rv
        if status == RETRY_BREAK:
            return err_or_rv
        if status == RETRY_RESET:
            backoff = opts['backoff']
            count = wait = 0
        if status == RETRY_CONTINUE:
            if opts['max_attempts'] > 0 and count >= opts['max_attempts']:
                raise RetryMaxAttemptsError(
                    opts['max_attempts'], reason=err_or_rv)
            wait = (backoff + backoff * retry_jitter) * opts['constant_factor']
            print "RETRIED IN ... %s" % wait
            if backoff > opts['max_backoff']:
                backoff = opts['max_backoff']
        gevent.sleep(wait)

