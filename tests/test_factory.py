import pytest
import pypel.processes as proc
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

@pytest.fixture
def factory():
    return proc.ProcessFactory()


def test_factory_valid_processor(factory):
    expected = proc.BaseProcess()
    obtained = factory.create_process()
    assert isinstance(obtained, type(expected))
    expected_props = [getattr(expected, prop) for prop in dir(expected) if "_" not in prop]
    obtained_props = [getattr(obtained, prop) for prop in dir(obtained) if "_" not in prop]
    assert expected_props == obtained_props

