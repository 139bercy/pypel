import pytest
import pypel


class Extractor2(pypel.Extractor):
    pass


class FakeExtractor:
    pass


class TestProcessInstanciation:
    def test_default_instanciates(self):
        pypel.Process()

    def test_bad_type_args_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(extractor="")
        with pytest.raises(ValueError):
            pypel.Process(transformer="")
        with pytest.raises(ValueError):
            pypel.Process(loader="")  # noqa
        with pytest.raises(ValueError):
            pypel.Process(extractor=[])

    def test_instance_from_bad_class_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(extractor=FakeExtractor())

    def test_good_args_instanciates(self):
        pypel.Process(extractor=Extractor2())

    def test_list_of_transformers_instanciates(self):
        pypel.Process(transformer=[pypel.Transformer(), pypel.Transformer()])

    def test_list_of_uninstanced_transformers_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(transformer=[pypel.Transformer, pypel.Transformer])

    def test_list_of_wrong_type_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(transformer=[0, 0])
