import datetime

# flush=True helps when running in a container without a tty attached
# (alternatively, "python -u" or PYTHONUNBUFFERED will help here)
def log(level, msg):
    """Logs a message with a timestamp and level."""
    now = datetime.datetime.now(datetime.UTC)
    print('{} | {} | {}'.format(now, level.ljust(5), msg), flush=True)

def debug(msg, is_debug_enabled=False):
    """Logs a debug message if debug mode is enabled."""
    if is_debug_enabled:
        log('DEBUG', msg)

def error(msg):
    """Logs an error message."""
    log('ERROR', msg)

def info(msg):
    """Logs an info message."""
    log('INFO', msg)

def verbose(msg, is_verbose_enabled=False):
    """Logs a verbose message if verbose mode is enabled."""
    if is_verbose_enabled:
        log('VERB', msg)