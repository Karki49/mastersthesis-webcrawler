import datetime
import logging

# ch = logging.StreamHandler()
# ch.setLevel(level=logging.INFO)
# ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))

_dt = datetime.datetime.utcnow()
fh = logging.FileHandler(filename=f'/tmp/crawler_{_dt.strftime("%Y-%m-%d_%Hh%Mm%Ss")}_UTC.log')
fh.setLevel(level=logging.INFO)
fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))

logger = logging.getLogger('crawlerapp')
logger.setLevel(level=logging.DEBUG)
# logger.addHandler(hdlr=ch)
logger.addHandler(hdlr=fh)


class Interval:
    def __init__(self):
        self.secs: float = None
        self.microsecs: float = None
        self.__t1 = None

    def __enter__(self):
        self.__t1 = datetime.datetime.now()
        return self

    def __exit__(self, *args):
        t2 = datetime.datetime.now()
        self.secs = (t2 - self.__t1).total_seconds()
        self.microsecs = int(self.secs * 1000 * 1000)  # rounded off to the nearest micro second


if __name__ == '__main__':
    logger.debug('This is a test')
    logger.info('This is a test')
    logger.warning('This is a test')
    logger.error('This is a test')
    logger.critical('This is a test')
