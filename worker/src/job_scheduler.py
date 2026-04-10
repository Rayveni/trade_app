from datetime import datetime
from time import sleep


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED


class JobScheduler:
    def __init__(self, topics_config: dict, functions_dict: dict):
        __slots__ = ['job_name_params', 'scheduler', 'job_id_job_names']

        """
        self.cron_triggers = {
            'every10seconds': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='*/10'),
            'every1second': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='*/1'),
            'every20seconds': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='*/20'),
            'everyminute': CronTrigger(year='*', month='*', day='*', hour='*', minute='*', second='0'),
            'every5minutes': CronTrigger(year='*', month='*', day='*', hour='*', minute='*/5', second='0'),
            'everyhour': CronTrigger(year='*', month='*', day='*', hour='*', minute='0', second='0'),
            'every2hours': CronTrigger(year='*', month='*', day='*', hour='*/2', minute='0', second='0'),
        }
        """
        # job params and stats
        self.job_name_params = {}
        for queue_name, queue_params in topics_config.items():
            self.job_name_params[f'{queue_name}_process'] = {
                'trigger': CronTrigger(**queue_params['listener_cron']),
                #'trigger': 'interval',
                #'seconds': 1,
                'func': functions_dict[f'{queue_name}_listener'],
                'max_instances': queue_params['max_instances'],
                'run_on_start': True,
            }

        self.scheduler = None
        # reverse lookup
        self.job_id_job_names = {}

    def schedule(self):
        # logger.debug(f'()')
        self.scheduler = BackgroundScheduler()
        # self.scheduler.add_listener(self.listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)
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
                        job.modify(next_run_time=datetime.now())
                        break
        while True:
            sleep(1)
