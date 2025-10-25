from backend.common_libs.redis_wrapper import redis_steams,redis_dict
from os import getenv
from ast import literal_eval

redis_topics=literal_eval(getenv('redis_topics'))
redis_url,app_name,redis_task_status=getenv('redis_url'),getenv('app_name'),getenv('redis_task_status')
class redis_jobs:
    __slots__ = ('redis_conn','redis_topics')
    def __init__(self)->None:
        self.redis_conn = redis_steams(getenv('redis_url'))
        self.redis_topics=literal_eval(getenv('redis_topics'))
    
    def __base_read_queue(self,topic:str,consumer_group:str,process_message,count:int=1)->str:
        ready_to_process_messages=self.redis_conn.consume(topic,consumer_group,count)
        if ready_to_process_messages is None:
            return f'Topic {topic} empty'
        for _message in ready_to_process_messages:
            process_message(_message)
            self.redis_conn.commit(topic,consumer_group,_message['message_id'])
        return f'Topic {topic} processed: {len(ready_to_process_messages)}'  
    
    def read_app_queue(self)->str:
        queue_params=self.redis_topics['app_topic']
        return self.__base_read_queue(queue_params['name'],queue_params['consumer_group'],self.__process_app_queue_message,queue_params['n_bulk'])
    
    def __process_app_queue_message(self,queue_message)->None:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print(redis_url,app_name,redis_task_status)
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print(queue_message)
        return redis_dict(redis_url,app_name).dict_add_key(dict_name=redis_task_status,key=queue_message['message_id'],value=queue_message['message'])
        
    

    

#read_tasks_queue