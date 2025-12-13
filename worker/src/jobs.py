from backend.common_libs.redis_wrapper import redis_steams,redis_dict
from os import getenv
from ast import literal_eval
from json import dumps
from datetime import datetime

#redis_topics=literal_eval(getenv('redis_topics'))
redis_url,app_name=getenv('redis_url'),getenv('app_name')
redis_task_status,redis_task_original_message=getenv('redis_task_status'),getenv('redis_task_original_message')
class redis_jobs:
    __slots__ = ('redis_conn','redis_topics')
    def __init__(self)->None:
        self.redis_conn = redis_steams(getenv('redis_url'))
        self.redis_topics=literal_eval(getenv('redis_topics'))
    
    def __base_read_queue(self,topic:str,consumer_group:str,queue_name:str,count:int=1)->str:
        ready_to_process_messages=self.redis_conn.consume(topic,consumer_group,count)
        if ready_to_process_messages is None:
            return f'Topic {topic} empty'
        for _message in ready_to_process_messages:
            self.__process_queue_message(_message,queue_name)
            self.redis_conn.commit(topic,consumer_group,_message['message_id'])
        return f'Topic {topic} processed: {len(ready_to_process_messages)}'  
    
    def read_app_queue(self,topic:str='app_topic')->str:
        queue_params=self.redis_topics[topic]
        n_bulk=queue_params.get('n_bulk',1)
        return self.__base_read_queue(queue_params['name'],queue_params['consumer_group'],topic,n_bulk)
    
    def __process_queue_message(self,queue_message,queue_name)->None:
        now_datetime=datetime.now().isoformat()
        if queue_name=='app_topic':
            status_message={'status':'new_task',
                            'created':now_datetime,
                            'updated':now_datetime
                            }
            
            message_id=queue_message['message_id']
            print (message_id)
            redis_dict(redis_url,app_name).dict_add_key(dict_name=redis_task_status,key=message_id,value=dumps(status_message))
            redis_dict(redis_url,app_name).dict_add_key(dict_name=redis_task_original_message,key=message_id,value=queue_message['message'])
    
    
            res=redis_steams(redis_url).publish(self.redis_topics['tasks_topic']['name'],{'topic':queue_name,'key':str(message_id)})
        else:
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print(queue_message)
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


        return 'success'
        
    

    

#read_tasks_queue