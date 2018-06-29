class Processor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def process(self, broker_message):
        raise NotImplementedError
