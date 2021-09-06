import pytest
import pypel


@pytest.fixture
def factory():
    return pypel.ProcessFactory()


class TestFactory:
    def test_factory_defaults_when_empty_config(self, factory, es_instance):
        expected = pypel.processes.Process()
        obtained = factory.create_process({}, es_instance=es_instance)
        assert isinstance(obtained, type(expected))

    def test_factory_instantiates_non_default_transformers(self, factory, es_instance):
        expected = pypel.processes.Process(transformer=pypel.transformers.Transformer)
        obtained = factory.create_process({"Extractors": {"name": "pypel.extractors.Extractor"},
                                           "Transformers": {"name": "pypel.transformers.Transformer"},
                                           "Loaders": {"name": "pypel.loaders.Loader"}}, es_instance=es_instance)
        assert isinstance(obtained, type(expected))

    def test_factory_instanciates_list_of_transformers(self, factory, es_instance):
        expected = pypel.processes.Process(transformer=[pypel.transformers.ColumnStripperTransformer(),
                                                        pypel.transformers.ColumnCapitaliserTransformer()])
        obtained = factory.create_process({"Extractors": {"name": "pypel.extractors.Extractor"},
                                           "Transformers": [{"name": "pypel.transformers.ColumnStripperTransformer"},
                                                            {"name": "pypel.transformers.Transformer"}],
                                           "Loaders": {"name": "pypel.loaders.Loader"}}, es_instance=es_instance)
        assert isinstance(obtained, type(expected))

    def test_raises_if_class_not_found(self, factory, es_instance):
        with pytest.raises(ValueError):
            factory.create_process({"Extractors": {"name": "pypel.Jesus"}}, es_instance)
