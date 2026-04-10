from sys import path

# appending a path
path.append('/worker/common_libs')
from queue_interface import base_queue


class BaseOperations:
    def __init__(self, msg_broker_settings: dict, db_creds: dict):

        self.msg_broker = base_queue(**msg_broker_settings)
