
from sys import path
# appending a path
path.append('/app/common_libs')
from redis_wrapper import redis_steams


class MessageBroker:
    __slots__ = ['driver', 'conn_settings']

    def __init__(self, driver: str, conn_settings: str):
        self.driver = driver
        self.conn_settings = conn_settings

    def publish(self, topic: str, header: str, message: str):
        if self.driver == 'redis':
            res = redis_steams(self.conn_settings).publish(topic, message, header)
        return res
