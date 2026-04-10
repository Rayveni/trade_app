import json
import importlib.util
from pathlib import Path

redis_driver_path, redis_class = (
    Path(__file__).parent.resolve() / 'redis_wrapper.py',
    'redis_steams',
)


class base_queue:
    __slots__ = ['driver_name', 'driver']

    def __init__(self, driver: str, connection_setting: dict):

        self.driver = driver
        if driver == 'redis':
            driver_class = self.__get_class_from_file(redis_driver_path, redis_class)
            self.driver = driver_class(**connection_setting)

    def __get_class_from_file(self, file_path: str, class_name: str):
        """
        Dynamically loads a class from a specified file path.
        """
        module_name = class_name
        # Create a module specification from the file path
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            raise ImportError(f'Could not find spec for file: {file_path}')
        # Create a new module from the specification
        module = importlib.util.module_from_spec(spec)
        # Execute the module's code
        spec.loader.exec_module(module)

        # Get the class from the loaded module using getattr
        return getattr(module, class_name)

    def conn_info(self):
        return self.driver.info()

    def create_consumer_group(self, topic: str, consumer_group: str):
        return self.driver.create_consumer_group(topic, consumer_group)

    def consumer_group_info(self, topic: str) -> bool:
        return self.driver.consumer_group_info(topic)

    def topic_info(self, topic: str):
        return self.driver.topic_info(topic)

    def clear_topic(self, topic: str) -> list:
        return self.driver.clear_topic(topic)

    def __encode_message(self, message: dict, header: dict) -> dict:
        return {'message': json.dumps(message), 'header': json.dumps(header)}

    def __decode_message(self, message: dict) -> dict:
        message['topic'] = message['topic'].decode()
        message['message_id'] = message['message_id'].decode()
        message['header'] = json.loads(message['message'][b'header'])
        message['message'] = json.loads(message['message'][b'message'])

        return message

    def publish(self, topic: str, message: dict, header: dict):
        return self.driver.publish(topic, self.__encode_message(message, header))

    def bulk_publish(self, topic: str, message_list: list):
        return self.driver.bulk_publish(topic, message_list)

    def consume(
        self,
        topic: str,
        consumer_group: str,
        count: int = 1,
        consumer: str = 'default_consumer',
        decode_message: bool = True,
    ) -> list:
        consumed_messages = self.driver.consume(topic, consumer_group, count, consumer)
        if consumed_messages is not None and decode_message:
            return list(map(self.__decode_message, consumed_messages))
        return consumed_messages

    def commit(self, topic: str, consumer_group: str, message_id: str) -> list:
        return self.driver.commit(topic, consumer_group, message_id)

    def get_uncommited_messages(
        self, topic: str, consumer_group: str, count: int = 10
    ) -> list:
        return self.driver.get_uncommited_messages(topic, consumer_group, count)

    def delete_all(self) -> bool:
        self.driver.delete_all()
        return True
