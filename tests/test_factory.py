import pytest
import pypel


@pytest.fixture
def factory():
    return pypel.ProcessFactory()


def test_factory_valid_processor(factory):
    expected = pypel.Process()
    obtained = factory.create_process({"class": "pypel.Process"}, "fake_indice")
    assert isinstance(obtained, type(expected))
