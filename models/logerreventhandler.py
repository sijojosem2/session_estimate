import logging
import traceback


class LogErrEventHandler:

    def __init__(self, filename):
        self.filename = filename

    def logwrite(self):
        """
        Initiates Logging file location
        """

        logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%d-%m-%Y:%H:%M:%S',
                            level=logging.DEBUG,
                            filename=self.filename)
        return logging.getLogger(__name__)

    def exceptionhandle(self, function):
        """
        Exception Handling decorator
        """

        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except:
                self.logwrite().error("uncaught exception: %s", traceback.format_exc())

        return wrapper