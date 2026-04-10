import importlib.util
from pathlib import Path

postgres_driver_path, pg_class = (
    Path(__file__).parent.resolve() / 'pg_wrapper.py',
    'pg_wrapper',
)


class db_class:
    __slots__ = ['driver_name', 'driver']

    def __init__(self, driver: str, connection_setting: dict) -> None:

        self.driver = driver
        if driver == 'postgresql':
            driver_class = self.__get_class_from_file(postgres_driver_path, pg_class)
            pg_conn = 'postgresql://{db_user}:{db_pswd}@{db_host}/{db_name}'.format(
                **connection_setting
            )
            self.driver = driver_class(pg_conn)

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

    def execute(self, query: str, query_params=None):
        return self.driver._execute(query, query_params)

    def insert_many(
        self, table: str, columns: list, values_list: list, conflict: list = []
    ):
        return self.driver.insert_many(table, columns, values_list, conflict)

    def execute_many(self, query: str, values_list: list):
        return self.driver.execute_many(query, values_list)

    def truncate(self, table: str):
        return self.driver.truncate(table)

    def fetch_all(self, query: str, return_type: str = 'json') -> tuple:
        return self.driver.fetch_all(query, return_type)

    def create_values_string(
        self, array: list, conv_to_json_index: list = [], nostringify_index: list = []
    ):
        return self.driver.create_values_string(
            array, conv_to_json_index, nostringify_index
        )
