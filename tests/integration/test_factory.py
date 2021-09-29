import pytest
import pypel


@pytest.fixture
def factory():
    return pypel.ProcessFactory()


class TestFactory:
    def test_factory_defaults_when_empty_config(self, factory):
        expected = pypel.processes.Process()
        obtained = factory.create_process({})
        assert isinstance(obtained, type(expected))

    def test_factory_instantiates_non_default_transformers(self, factory):
        expected = pypel.processes.Process(transformer=pypel.transformers.Transformer)
        obtained = factory.create_process({"Transformers": [{"name": "pypel.transformers.Transformer"}]})
        assert isinstance(obtained, type(expected))

    def test_factory_instanciates_list_of_transformers(self, factory):
        expected = pypel.processes.Process(transformer=[pypel.transformers.ColumnStripperTransformer(),
                                                        pypel.transformers.ColumnCapitaliserTransformer()])
        obtained = factory.create_process({"Transformers": [{"name": "pypel.transformers.ColumnStripperTransformer"},
                                                            {"name": "pypel.transformers.Transformer"}]})
        assert isinstance(obtained, type(expected))

    def test_raises_if_class_not_found(self, factory):
        with pytest.raises(ValueError):
            factory.create_process({"Extractor": {"name": "pypel.Jesus"},
                                    "Transformers": [{"name": ""}],
                                    "Loader": {"name": "",
                                               "indice": ""}})
