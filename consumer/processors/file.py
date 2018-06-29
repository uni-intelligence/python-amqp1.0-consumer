import json
import os
import sys
from collections import defaultdict
from datetime import datetime

import pytz


class Processor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def process(self, broker_message):
        raise NotImplementedError


class FileProcessor(Processor):
    """
    Process message by writing it to a given file. By default stdout is used.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if kwargs.get('file', '-') == '-':
            self.out_file = sys.stdout
        else:
            self.out_file = open(kwargs['file'], 'w')

        self.indent = int(kwargs['indent']) if 'indent' in kwargs else None

    def process(self, broker_message):
        print(json.dumps(broker_message, indent=self.indent), file=self.out_file)


class MultiFileProcessor(Processor):
    """
    Generic class for creating multi-file output. Each message is stored in a separate file called <num>.json
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._message_counter = defaultdict(int)
        self.indent = int(kwargs['indent']) if 'indent' in kwargs else None

    def get_dir(self, broker_message):
        return self.kwargs.get('dir', '.')

    def process(self, broker_message):
        directory = self.get_dir(broker_message)

        if not os.path.exists(directory):
            os.makedirs(directory)

        self._message_counter[directory] += 1

        file_path = os.path.join(directory, str(self._message_counter[directory]) + '.json')

        with open(file_path, 'w') as f:
            print(json.dumps(broker_message, indent=self.indent), file=f)


class HourlyMultiFileProcessor(MultiFileProcessor):
    """
    Splits saving of messages files into structure of directories like yyyy-mm-dd/hh/<num>.json
    """

    def get_dir(self, broker_message):
        now = datetime.now(tz=pytz.utc)
        return os.path.join(super().get_dir(broker_message), now.strftime('%Y-%m-%d'), now.strftime('%H'))
