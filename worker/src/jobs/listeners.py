with open('/worker/worker/src/jobs/sql/create_task.sql', 'r', encoding='utf-8') as file:
    insert_task_sql = file.read()


def front_queue_listener(msg_broker, db_driver, consumer_params: dict, logger):
    logger.debug('read_front_queue')
    msg_list = msg_broker.consume(
        consumer_params['topic'],
        consumer_params['consumer_group'],
        consumer_params['n_bulk'],
    )
    if msg_list is not None:
        task_status_values, task_detail_values, msg_ids, header_ids = [], [], [], []
        for _msg in msg_list:
            task_status_values.append(
                db_driver.create_values_string([_msg['header']['id']])
            )
            task_detail_values.append(
                db_driver.create_values_string(
                    [
                        _msg['header']['id'],
                        _msg['topic'],
                        _msg['message_id'],
                        _msg['message'],
                        _msg['header'],
                    ],
                    [3, 4],
                )
            )
            header_ids.append(_msg['header']['id'])
            msg_ids.append(_msg['message_id'])

        # logger.info(insert_task_sql.format(task_status_values=",".join(task_status_values),task_details_values=",".join(task_detail_values)))
        res = db_driver.execute(
            insert_task_sql.format(
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


def tasks_queue_listener(logger):
    logger.debug('read_tasks_queue')
