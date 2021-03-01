from pypel.processes.Process import Process


class ProcessFactory:
    def create_process(self, *args, **kwargs):
        return Process(*args, **kwargs)
