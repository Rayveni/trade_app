from job_scheduler import JobScheduler
from logger.legacy_logger import get_logger

from jobs.listeners import front_queue_listener, tasks_queue_listener
from os import getenv
import json
from sys import path

# appending a path
path.append('/worker/common_libs')
from utils import read_secrets
from queue_interface import base_queue
from db_interface import db_class
from functools import partial

task_db_creds_path = '/run/secrets/task_db_creds'
msg_broker = base_queue(
    driver='redis', connection_setting={'redis_url': getenv('redis_url')}
)
db_driver = db_class(
    driver='postgresql',
    connection_setting={
        **read_secrets(task_db_creds_path),
        **{'db_host': getenv('db_host')},
    },
)

with open('/worker/topics_config.json', 'r') as file:
    topics_config = json.load(file)

apscheduler_logger = get_logger('apscheduler', 'ERROR')
logger = get_logger(__name__)
logger.info('start')


def main():
    job_scheduler = JobScheduler(
        topics_config,
        {
            'front_topic_listener': partial(
                front_queue_listener,
                msg_broker=msg_broker,
                db_driver=db_driver,
                consumer_params={
                    'topic': 'front_topic',
                    'consumer_group': topics_config['front_topic']['consumer_group'],
                    'n_bulk': topics_config['front_topic']['n_bulk'],
                },
                logger=logger,
            ),
            'tasks_topic_listener': partial(tasks_queue_listener, logger=logger),
        },
    )
    job_scheduler.schedule()


if __name__ == '__main__':
    main()
