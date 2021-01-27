from pypel.processes.Process import Process as BaseProcess


class ProcessFactory:
    def create_process(self, *args, **kwargs):
        return BaseProcess(*args, **kwargs)
