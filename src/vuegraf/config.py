"""
Configuration-related functions for Vuegraf.
"""
import signal
import src.vuegraf.log as logger

# Global variables that will be set by the main module
running = False
pause_event = None

def handle_exit(signum, frame):
    """
    Handle exit signals.
    
    Args:
        signum: The signal number.
        frame: The current stack frame.
    """
    global running
    logger.error('Caught exit signal')
    running = False
    pause_event.set()

def get_config_value(config, key, default_value):
    """
    Get a configuration value with a default.
    
    Args:
        config: The configuration dictionary.
        key: The key to look up.
        default_value: The default value to return if the key is not found.
        
    Returns:
        The configuration value, or the default value if not found.
    """
    if key in config:
        return config[key]
    return default_value

def setup_signal_handlers():
    """
    Set up signal handlers for graceful shutdown.
    """
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGHUP, handle_exit)