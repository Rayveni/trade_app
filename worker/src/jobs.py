from backend.common_libs.redis_wrapper import redis_steams
from os import getenv
from ast import literal_eval

redis_topics=literal_eval(getenv('redis_topics'))

class redis_jobs:
    __slots__ = ('redis_conn','redis_topics')
    def __init__(self)->None:
        self.redis_conn = redis_steams(getenv('redis_url'))
        self.redis_topics=literal_eval(getenv('redis_topics'))
    
    def __base_read_queue(self,topic:str,consumer_group:str,job_function,count:int=1)->str:
        ready_to_process_messages=self.redis_conn.consume(topic,consumer_group,count)
        if ready_to_process_messages is None:
            return f'Topic {topic} empty'
        for _message in ready_to_process_messages:
            job_function(_message)
            self.redis_conn.commit(topic,consumer_group,_message['message_id'])
        return f'Topic {topic} processed: {len(ready_to_process_messages)}'  
    
    def read_app_queue(self)->str:
        queue_params=self.redis_topics['app_topic']
        f=lambda x:1
        return self.__base_read_queue(queue_params['name'],queue_params['consumer_group'],f,10)
        
    

    

#read_tasks_queue