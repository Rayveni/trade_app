import datetime
import logging
import random
import time

from jobs import redis_jobs
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED

apscheduler_logger = logging.getLogger('apscheduler')
r_jobs=redis_jobs()
# decomment to skip messages from apscheduler
#apscheduler_logger.setLevel(logging.ERROR)

logging.basicConfig(
    format='%(asctime)s %(levelname)8s [%(filename)-10s%(funcName)20s():%(lineno)04s] %(message)s',
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

def read_app_queue():
    res=r_jobs.read_app_queue(topic='app_topic')
    logger.debug(f'function read_app_queue: run {res}')

def read_tasks_queue():
    res=r_jobs.read_app_queue(topic='tasks_topic')
    logger.debug(f'function read_tasks_queue: run {res}')

class JobScheduler:
    def __init__(
        self
    ):

        self.cron_triggers = {
            'every10seconds': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='*/10'),
            'every20seconds': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='*/20'),
            'everyminute': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='0'),
            'every5minutes': CronTrigger(year='*', month='*', day='*', hour='*', minute='*/5', second='0'),
            'everyhour': CronTrigger(year='*', month='*', day='*', hour='*', minute='0', second='0'),
            'every2hours': CronTrigger(year='*', month='*', day='*', hour='*/2', minute='0', second='0'),
        }

        # job params and stats
        self.job_name_params = {
            'task_app_process': {
                'trigger': self.cron_triggers['every10seconds'],
                #'trigger': 'interval',
                #'seconds': 1,
                'func': read_app_queue,    
                'max_instances': 1,
                'run_on_start': True,
            },
            'task_process': {
                'trigger': self.cron_triggers['every20seconds'],
                #'trigger': 'interval',
                #'seconds': 1,
                'func': read_tasks_queue,    
                'max_instances': 1,
                'run_on_start': True,
            },            
        }
        self.scheduler = None
        # reverse lookup
        self.job_id_job_names = {}
        
    def schedule(self):
        #logger.debug(f'()')
        self.scheduler = BackgroundScheduler()
        #self.scheduler.add_listener(self.listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        self.scheduler.start()
        # add jobs to scheduler
        for job_name, params in self.job_name_params.items():
            run_on_start = params.pop('run_on_start')
            job = self.scheduler.add_job(**params)
            # for reverse lookup in listener
            self.job_id_job_names[job.id] = job_name
            # start immediately
            if run_on_start:
                for job in self.scheduler.get_jobs():
                    if job.name == job_name:
                        job.modify(next_run_time=datetime.datetime.now())
                        break
        while True:       
            time.sleep(1)


def main():
    job_scheduler = JobScheduler()
    job_scheduler.schedule()

if __name__ == '__main__':
    main()


""" 
1.получение списка дат
"""