import datetime
import logging
import random
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED

apscheduler_logger = logging.getLogger('apscheduler')
# decomment to skip messages from apscheduler
#apscheduler_logger.setLevel(logging.ERROR)

logging.basicConfig(
    format='%(asctime)s %(levelname)8s [%(filename)-10s%(funcName)20s():%(lineno)04s] %(message)s',
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)



def backup():
    logger.debug(f'backup: start ...')
    # do something here
    time.sleep(15)
    #time.sleep(2)
    if random.randint(0, 3) == 0:
        raise Exception('BACKUP PROBLEM')
    logger.debug(f'backup: ready')

class JobScheduler:
    def __init__(
        self,
        var_a,
        var_b,
    ):
        self.var_a = var_a
        self.var_b = var_b

        self.cron_triggers = {
            'every10seconds': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='*/10'),
            'everyminute': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='0'),
            'every5minutes': CronTrigger(year='*', month='*', day='*', hour='*', minute='*/5', second='0'),
            'everyhour': CronTrigger(year='*', month='*', day='*', hour='*', minute='0', second='0'),
            'every2hours': CronTrigger(year='*', month='*', day='*', hour='*/2', minute='0', second='0'),
        }

        # job params and stats
        self.job_name_params = {
            'job_backup': {
                'trigger': self.cron_triggers['every10seconds'],
                #'trigger': 'interval',
                #'seconds': 1,
                'func': backup,    
                'max_instances': 1,
                'run_on_start': True,
            },
        }
        self.job_name_stats = {
            'job_backup': {
                # run counters (unsafe)
                'execd': 0,
                'error': 0,
                'missd': 0,
            },
        }
        self.scheduler = None
        # reverse lookup
        self.job_id_job_names = {}

    def listener(self, event):
        logger.debug(f'event = {event}')
        job = self.scheduler.get_job(event.job_id)
        job_name = self.job_id_job_names[job.id]
        if event.code == EVENT_JOB_EXECUTED:
                self.job_name_stats[job_name]['execd'] += 1
        elif event.code == EVENT_JOB_ERROR:
                self.job_name_stats[job_name]['error'] += 1
        elif event.code == EVENT_JOB_MISSED:
                self.job_name_stats[job_name]['missd'] += 1
        if event.exception:
            logger.debug(f'{job_name}: job exception event, exception = {event.exception}')
        
    def schedule(self):
        logger.debug(f'()')
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_listener(self.listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)
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

        et_start_secs = int(time.time())
        while True:
            run_secs = int(time.time()) - et_start_secs
            for job_name, stats in self.job_name_stats.items():
                execd, error, missd = stats['execd'], stats['error'], stats['missd']
                logger.debug(f'{run_secs:3} {job_name:15s} execd: {execd:3}   error: {error:3}   missd: {missd:3}')
            time.sleep(1)

def main():
    job_scheduler = JobScheduler(
        var_a='a',
        var_b='b',
    )
    job_scheduler.schedule()

if __name__ == '__main__':
    main()


"""
1.получение списка дат
"""