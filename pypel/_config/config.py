"""
Package-wide configuration & its API

Configuration is stored inside a dictionnary where the config's name is the key, and the parameter the value.
Example :
>>> {"LOGS": True}
"""
from typing import Dict, Any


_conf = {"LOGS": True,
         "LOGS_LEVEL": "DEBUG"}


def set_config(**kwargs) -> None:
    """
    Updates _conf with the passed **keywords arguments**.

    Example :
    >>>set_config(LOGS=True, LOGS_LEVEL="CRITICAL")
    Sets `pypel`'s logs level to `logging.CRITICAL`
    """
    _conf.update(kwargs)


def get_config() -> Dict[str, Any]:
    """Returns a **soft** copy of the configuration. You should never modify `get_config`'s return directly."""
    return _conf
