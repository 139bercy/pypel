from pypel import get_config, set_config


def test_default_config_getter():
    assert get_config() == {"LOGS": True, "LOGS_LEVEL": "INFO"}


def test_config_setter():
    set_config(LOGS=False)
    assert get_config() == {"LOGS": False, "LOGS_LEVEL": "INFO"}
    set_config(LOGS=True)


def test_disable_logs_fixture(disable_logs):
    assert get_config() == {"LOGS": False, "LOGS_LEVEL": "INFO"}
