from pypel.processes.Process import Process as BaseProcess
from pypel.processes.ExcelProcess import ExcelProcess


class ProcessFactory:

    def __init__(self):
        self._processes = {
            "DummyProcess": BaseProcess,
            "DummyExcelProcess": ExcelProcess
        }

    def create_process(self, process_name, elastic_indice):
        process = self._processes.get(process_name)
        if not process:
            raise ValueError(f"{process_name} not found in ProcessFactory")
        return process(elastic_indice)
