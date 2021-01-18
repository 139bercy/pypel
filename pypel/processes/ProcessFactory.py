import pypel.processes.Process as BaseProcess


class ProcessFactory:

    def __init__(self):
        self._processes = {
            "DummyProcess": BaseProcess.Process
        }

    def create_process(self, process_name, elastic_indice):
        process = self._processes.get(process_name)
        if not process:
            raise ValueError(f"{process_name} not found in ProcessFactory")
        return process(elastic_indice)
