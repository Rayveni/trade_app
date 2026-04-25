from .jobs import Jobs
from functools import partial


class queue_listener:
    __slots__ = (
        'sql_dict',
        'msg_broker',
        'task_db_driver',
        'main_db_driver',
        'queues_params',
        'logger',
        'jobs'
    )

    def __init__(
        self,
        msg_broker,
        task_db_driver,
        main_db_driver,
        queues_params: dict,
        logger,
        sql_path: str = '/worker/worker/src/jobs/sql/tasks/{}.sql',
    ):
        self.sql_dict = {}
        for _sql_name in ['create_task', 'update_task_status']:
            with open(sql_path.format(_sql_name), 'r', encoding='utf-8') as file:
                # self.insert_task_sql = file.read()
                self.sql_dict[_sql_name] = file.read()
        self.msg_broker = msg_broker
        self.task_db_driver = task_db_driver
        self.main_db_driver = main_db_driver
        self.queues_params = queues_params
        self.logger = logger
        self.jobs=Jobs(self.msg_broker,self.task_db_driver,self.main_db_driver,self.logger)

    def _process_msg_list_new_task(self, msg_list: list) -> None:
        task_status_values, task_detail_values, header_ids, msg_ids = [], [], [], []
        for _msg in msg_list:
            task_status_values.append(self.task_db_driver.create_values_string([_msg['header']['id']]))
            task_detail_values.append(
                self.task_db_driver.create_values_string(
                    array=[
                        _msg['header']['id'],
                        _msg['topic'],
                        _msg['message_id'],
                        _msg['message'],
                        _msg['header'],
                    ],
                    conv_to_json_index=[3, 4],
                )
            )
            header_ids.append(_msg['header']['id'])
            msg_ids.append(_msg['message_id'])

        res = self.task_db_driver.execute(
            self.sql_dict['create_task'].format(
                task_status_values=','.join(task_status_values),
                task_details_values=','.join(task_detail_values),
            )
        )
        self.logger.info(f'inserted in db res={res} messages:topic={_msg["topic"]} message_ids={",".join(msg_ids)} header_ids={",".join(header_ids)}')


    def _process_msg_list_commit(self, queue_name: str, msg_id_list: list) -> None:
        for _msg_id in msg_id_list:
            self.msg_broker.commit(
                self.queues_params[queue_name]['topic'],
                self.queues_params[queue_name]['consumer_group'],
                _msg_id,
            )
        self.logger.info(f'commited message_ids={",".join(msg_id_list)}')

    def _process_move_tasks_to_queue(self, target_queue: str, msg_list: list):
        res = self.msg_broker.bulk_publish(
            topic=self.queues_params[target_queue]['topic'],
            message_list=[
                {'message': msg['message'], 'header': msg['header']} for msg in msg_list
            ],
            encode_message=True,
        )
        self.logger.info(f'tasks moved to {target_queue} queue,res={res}')

    def base_queue_listener(
        self,
        queue_name: str,
        apply_function,
        return_msg_list: bool = False,
        bulk_process: bool = False,
    ) -> tuple:
        """
        returs true if readed and processed messages
        """

        self.logger.debug(f'read {queue_name}')
        msg_list = self.msg_broker.consume(
            self.queues_params[queue_name]['topic'],
            self.queues_params[queue_name]['consumer_group'],
            self.queues_params[queue_name]['n_bulk'],
        )
        if msg_list is not None:
            if bulk_process:
                apply_function(msg_list)
            else:
                for msg in msg_list:
                    apply_function(msg)
                                
            self._process_msg_list_commit(queue_name, [_msg['message_id'] for _msg in msg_list])


            if return_msg_list:
                res = (True, msg_list)
            else:
                res = (True, None)
        else:
            res = (False, None)
        return res

    def front_queue_listener(self):
        is_consumed, msg_list = self.base_queue_listener(
            queue_name='front_queue',
            apply_function=self._process_msg_list_new_task,
            return_msg_list=True,
            bulk_process=True,
        )
        if is_consumed:
            self._process_move_tasks_to_queue('back_queue', msg_list)

    def tasks_queue_listener(self):
        self.base_queue_listener(
            queue_name='back_queue',
            apply_function=partial(self._process_back_task, 'back_queue'),
            return_msg_list=False,
            bulk_process=False,
        )
    def __update_task_status(self,task_id:str,task_status:str,error_message:str='null')->None:
        self.task_db_driver.execute(self.sql_dict['update_task_status'].format(task_status=task_status,error_message=error_message,task_id=task_id))
        
    def _process_back_task(self, queue_name: str, msg: dict):
        self.logger.info(f'~~~~~~~~~~~~~~~~~~~~{msg}')
        # update status start self.sql_dict['update_task_status']
        task_id=msg['header']['id']
        self.__update_task_status(task_id=task_id,task_status='PROGRESS')
        method_name='first_task'  
        result = getattr( self.jobs, method_name)("World")
        self.__update_task_status(task_id=task_id,task_status='DONE')

        self._process_msg_list_commit(queue_name, [msg['message_id']])
        
        

