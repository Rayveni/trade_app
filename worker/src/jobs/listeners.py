class queue_listener:
    __slots__ = ['insert_task_sql']
    def __init__(self,insert_task_sql_path:str='/worker/worker/src/jobs/sql/create_task.sql'):
        with open(insert_task_sql_path, 'r', encoding='utf-8') as file:
            self.insert_task_sql = file.read()
            
    def front_queue_listener(self,msg_broker, db_driver, consumer_params: dict,producer_params:dict, logger):
        logger.debug('read_front_queue')
        #attempt to consume messages
        msg_list = msg_broker.consume(
            consumer_params['topic'],
            consumer_params['consumer_group'],
            consumer_params['n_bulk'],
        )
        if msg_list is not None:
            logger.info(msg_list)
            #prepare sql expression to insert
            task_status_values, task_detail_values, msg_ids, header_ids = [], [], [], []
            for _msg in msg_list:
                task_status_values.append(
                    db_driver.create_values_string([_msg['header']['id']])
                )
                task_detail_values.append(
                    db_driver.create_values_string(
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

            res = db_driver.execute(
                self.insert_task_sql.format(
                    task_status_values=','.join(task_status_values),
                    task_details_values=','.join(task_detail_values),
                )
            )
            logger.info(
                f'inserted in db res={res} messages:topic={_msg["topic"]} message_ids={",".join(msg_ids)} header_ids={",".join(header_ids)}'
            )
            for _msg_id in msg_ids:
                msg_broker.commit(
                    consumer_params['topic'], consumer_params['consumer_group'], _msg_id
                )
            logger.info(f'commited message_ids={",".join(msg_ids)}')

        #move message to task_queue
            res = msg_broker.bulk_publish(topic=producer_params['topic'], 
                                          message_list=[{'message':msg['message'],'header':msg['header']} for msg in msg_list],
                                          encode_message=True)
        ####
            
    def tasks_queue_listener(self,msg_broker, db_driver,consumer_params:dict,logger):
        logger.debug('read_tasks_queue')
        msg_list = msg_broker.consume(
            consumer_params['topic'],
            consumer_params['consumer_group'],
            consumer_params['n_bulk'],
        )
        if msg_list is not None:
            for _msg in msg_list:
                logger.info(f'{_msg}')
    