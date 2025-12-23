from backend.common_libs.redis_wrapper import redis_steams,redis_dict

from backend.common_libs.pg_wrapper import pg_wrapper
#from moex_etl import get_unbound_moex_query
from etl.etl_jobs import etl_jobs
from os import getenv
from ast import literal_eval
import json
from datetime import datetime

#redis_topics=literal_eval(getenv('redis_topics'))
redis_url,app_name=getenv('redis_url'),getenv('app_name')
redis_task_status,redis_task_original_message=getenv('redis_task_status'),getenv('redis_task_original_message')

class redis_jobs:
    __slots__ = ('redis_conn','redis_topics','pg_conn','etl_job')
    def __init__(self)->None:
        self.redis_conn = redis_steams(getenv('redis_url'))
        self.redis_topics=literal_eval(getenv('redis_topics'))
        self.pg_conn=pg_wrapper(getenv('pg_url'))
        self.etl_job=etl_jobs(self.pg_conn,
                              int(getenv('moex_limit')),
                              int(getenv('moex_n_concurrent')))

        
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


    def  __status_message(self,status:str,queue_name:str,created_time:str=None,n_iteration:int=0,error_message:str=None)->str:
        now_datetime=datetime.now().isoformat()
        if created_time is None:
            created_time=now_datetime     
        status_message={'status':status,
                        'queue_name':queue_name,
                        'created':created_time,
                        'n_iteration':n_iteration,
                        'error_message':error_message,
                        'updated':now_datetime
                        }  
        return json.dumps(status_message) 
    def __process_queue_message(self,queue_message,queue_name)->None:    
        message_header=json.loads(queue_message['header'])
        task_id=message_header['id']   
        
        print('~~~~~~~~~~~00000000000000000000~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print(queue_message)
        print('~~~~~~~~~~~~~~~00000000000000000000000~~~~~~~~~~~~~~~~~~~~~~')  
        queue_message_message=queue_message['message']               
        #message_id=queue_message['message_id']     
        if queue_name=='app_topic':            
            self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('new_task',queue_name))
            self.__add_redis_dict(dict_name=redis_task_original_message,key=task_id,value=queue_message_message)
            message_header['topic'] ='app_topic'  
            self.__add_new_task(queue_message_message,json.dumps(message_header))
        else:
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print(queue_message)
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')  
            self.__execute_task_message(json.loads(queue_message_message) ,message_header)  
        return 'success'
    
    def __execute_task_message(self,message:dict,header:dict)->None: 
        task_id,task_type=header['id'],header['type']    
        current_status_state=self.__get_task_status(task_id)
        self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('start processing',
                                                                                         header['topic'],
                                                                                         current_status_state['created']
                                                                                         ))

        if task_type=='upload_securities_dict':        
            success_flg,result=self.etl_job.execute_etl_job(task_type,message['task_params'])  
            if success_flg:
                
                end_flag=result['end_flag']
                if end_flag:
                    new_status_message='all iteraions are done'
                else:
                    new_status_message='iteration complete'
                    message['task_params']['start']=message['task_params']['start']+self.etl_job.moex_limit
                    message['task_params']['truncate']=False
                    header['topic']='tasks_topic'
                    self.__add_new_task(json.dumos(message),json.dumps(header))
                status_message=self.__status_message(new_status_message,header['topic'],current_status_state['created'],
                                                     n_iteration=current_status_state['n_iteration']+1)
                
            else:
                status_message=self.__status_message('iteration error',header['topic'],current_status_state['created'],
                                                     n_iteration=current_status_state['n_iteration']+1,error_message=result)
            
            self.__update_redis_task_status(redis_task_status,task_id,status_message)
            
    def __add_redis_dict(self,dict_name:str,key:str,value)->None:
        return redis_dict(redis_url,app_name).dict_add_key(dict_name=dict_name,key=key,value=value)
        
    def __update_redis_task_status(self,dict_name:str,key:str,new_value)->None:
        return redis_dict(redis_url,app_name).update_dict_value(dict_name=dict_name,key=key,new_value=new_value)
    
    def __get_task_status(self,task_id:str)->None:
        return json.loads(redis_dict(redis_url,app_name).get_dict(redis_task_status,task_id))
    
    def __get_task_info(self,task_id:str)->None:
        return json.loads(redis_dict(redis_url,app_name).get_dict(redis_task_original_message,task_id))    
    
    def __add_new_task(self,message:str,header:str)->None:
        return redis_steams(redis_url).publish(self.redis_topics['tasks_topic']['name'],message,header)
