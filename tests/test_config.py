import pytest
from pypel import get_config, set_config


def test_default_config_getter():
    assert get_config() == {"LOGS": True, "LOGS_LEVEL": "DEBUG"}


def test_config_setter():
    set_config(LOGS=False)
    assert get_config() == {"LOGS": False, "LOGS_LEVEL": "DEBUG"}
    set_config(LOGS=True)


def test_disable_logs_fixture(disable_logs):
    assert get_config() == {"LOGS": False, "LOGS_LEVEL": "DEBUG"}
