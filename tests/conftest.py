import pytest
import os
import json


@pytest.fixture
def params(get_conf):
    with open(os.path.join(get_conf, "parameters.json")) as f:
        params = json.load(f)
    return params


@pytest.fixture
def get_conf():
    get_conf = "conf/"
    return get_conf




