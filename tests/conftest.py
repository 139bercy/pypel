import pytest
import os
import json
from pypel import set_config, get_config


@pytest.fixture
def params(get_conf):
    with open(os.path.join(get_conf, "parameters_template.json")) as f:
        params = json.load(f)
    return params


@pytest.fixture
def get_conf():
    get_conf = "Doc/"
    return get_conf


@pytest.fixture
def disable_logs():
    set_config(LOGS=False)
    yield get_config()
    set_config(LOGS=True)
