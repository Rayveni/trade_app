import json
import logging
from logging import Formatter
from time import ctime
from os import getenv


class JsonFormatter(Formatter):
    def __init__(self):
        super(JsonFormatter, self).__init__()

    def format(self, record):
        json_record = {
            'created': ctime(record.__dict__['created']),
            'levelname': record.__dict__['levelname'],
        }
        json_record['message'] = record.getMessage()

        for _record in record.__dict__['custom_fields']:
            json_record[_record] = record.__dict__[_record]

        if record.levelno == logging.ERROR and record.exc_info:
            json_record['err'] = self.formatException(record.exc_info)
        return json.dumps(json_record)


logger = logging.root
# handler = logging.StreamHandler()
handler = logging.FileHandler('/app/logs/fastapi_template.log')
handler.setFormatter(JsonFormatter())
logger.handlers = [handler]
logger.setLevel(level=getenv('log_level', 'INFO'))


logging.getLogger('uvicorn.access').disabled = True
