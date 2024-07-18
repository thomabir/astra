import logging
from datetime import UTC, datetime


class LoggingHandler(logging.Handler):
    def __init__(self, instance):
        logging.Handler.__init__(self)
        self.instance = instance

    def emit(self, record):
        if record.levelno == logging.ERROR:
            self.instance.error_free = False

        # print(f"[{record.levelname}] {record.msg} {str(record.exc_info)}")

        dt_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname.lower()
        message = record.msg + " " + str(record.exc_info)

        # make message safe for sql
        message = message.replace("'", "''")

        self.instance.cursor.execute(
            f"INSERT INTO log VALUES ('{dt_str}', '{level}', '{message}')"
        )
