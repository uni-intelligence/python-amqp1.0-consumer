#!/usr/bin/env python3

import logging
import time

import xmltodict
from proton.handlers import MessagingHandler
from proton.reactor import Container

from helpers.import_name import import_name

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MessageHandler(MessagingHandler):
    def __init__(self, url, processors, *args, **kwargs):
        super(MessageHandler, self).__init__(*args, **kwargs)
        self.url = url
        self.processors = processors
        self.counter = 0
        self.total = 0
        self.last_timer = None
        self.period = 2

    def statistics(self):
        self.total += 1
        event_timer = int(time.time() / self.period)

        if event_timer == self.last_timer:
            self.counter += 1
        else:
            logger.info("Message receiving rate %.0f/s ; total %d" % (self.counter / self.period, self.total))
            self.counter = 1
            self.last_timer = event_timer

    def on_start(self, event):
        logger.info("Start listning on {}".format(self.url))
        event.container.create_receiver(self.url)

    def on_message(self, event):
        self.statistics()

        if event.message.body is None:
            logger.error(event)
        else:
            dict_body = xmltodict.parse(event.message.body)
            for processor in self.processors:
                processor.process(dict_body)

    def on_connection_error(self, error):
        logger.error(error)

    def on_session_error(self, error):
        logger.error(error)

    def on_link_error(self, error):
        logger.error(error)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--quiet', action='count', default=0,
                        help='Give less output. Option is additive, and can be used up to 3 times (corresponding to '
                             'WARNING, ERROR, and CRITICAL logging levels).')

    parser.add_argument('-p', '--processor', action='append')
    parser.add_argument('-D', metavar='<varname>=<value>', action='append')
    parser.add_argument('url')

    args = parser.parse_args()
    kwargs = {}

    if args.quiet == 1:
        logger.setLevel(logging.WARNING)

    elif args.quiet == 2:
        logger.setLevel(logging.ERROR)

    elif args.quiet >= 3:
        logger.setLevel(logging.CRITICAL)

    try:
        if args.D:
            kwargs = dict([v.split('=', 1) for v in args.D])
    except ValueError:
        parser.error("Error in parsing -D arguments.")

    if not args.processor:
        parser.error("Please specify at least one processor")

    processors = []
    try:
        for klass_name in args.processor:
            klass = import_name(klass_name)
            processors.append(klass(**kwargs))
    except (AttributeError, ModuleNotFoundError):
        parser.error("Cannot import at least one of processor.")

    try:
        Container(MessageHandler(url=args.url, processors=processors)).run()
    except KeyboardInterrupt as e:
        logger.warning("Finishing on keyboard action...")
