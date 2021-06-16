import pytest
from pypel import Process, Extractor


class TestProcess:
    def test_assert_instanciation_fails_if_extractor_not_instanced(self):
        with pytest.raises(ValueError):
            Process(extractor=Extractor)

    def test_instanciation_passes_if_extractor_is_instanced(self):
        Process(extractor=Extractor())

    def test_default_instanciates(self):
        Process()
