from backend.common_libs.redis_wrapper import redis_steams,redis_dict

from backend.common_libs.pg_wrapper import pg_wrapper
#from moex_etl import get_unbound_moex_query
from etl.etl_jobs import etl_jobs
from os import getenv
from ast import literal_eval
import json
from collections.abc import Callable
from datetime import date, timedelta,datetime
#redis_topics=literal_eval(getenv('redis_topics'))
redis_url,app_name=getenv('redis_url'),getenv('app_name')
redis_task_status,redis_task_original_message=getenv('redis_task_status'),getenv('redis_task_original_message')
date_format='%Y-%m-%d'

class redis_jobs:
    __slots__ = ('redis_conn','redis_topics','pg_conn','etl_job','logger')
    def __init__(self,logger)->None:
        self.redis_conn = redis_steams(getenv('redis_url'))
        self.redis_topics=literal_eval(getenv('redis_topics'))
        self.pg_conn=pg_wrapper(getenv('pg_url'))
        self.etl_job=etl_jobs(self.pg_conn,
                              int(getenv('moex_limit')),
                              int(getenv('moex_n_concurrent'))
                              )
        self.logger=logger

        
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
        
        
        queue_message_message=queue_message['message']  
        self.logger.info(f"queue={queue_name} step=0 message={queue_message}")             
        #message_id=queue_message['message_id']     
        if queue_name=='app_topic':                   
            self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('new_task',queue_name))
            self.__add_redis_dict(dict_name=redis_task_original_message,key=task_id,value=queue_message_message)
            message_header['topic'] ='app_topic'  
            self.__add_new_task(queue_message_message,json.dumps(message_header))
        else: 
            self.__execute_task_message(json.loads(queue_message_message) ,message_header)  
        return 'success'
    
    def __execute_task_message(self,message:dict,header:dict)->None: 
        task_id,task_type=header['id'],header['type']   
        current_status_state=self.__get_task_status(task_id)


        if task_type=='upload_securities_dict': 
            if current_status_state['n_iteration']==0:
                self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('start processing',
                                                                                                    header['topic'],
                                                                                                    current_status_state['created']
                                                                                                    ))            
                   
            success_flg,result=self.etl_job.execute_etl_job(task_type,message['task_params'])  
            self.logger.info(f"queue={header['topic']} step=1 message={result}")
            if success_flg:
                
                end_flag=result['end_flag']
                if end_flag:
                    new_status_message='all iterations are done'
                    header['type']='check_upload_securities_dict'
                    self.__add_new_task(json.dumps({}),json.dumps(header))
                    self.logger.info(f"queue={header['topic']} step=2 message={result}")
                else:
                    new_status_message='iteration complete'
                    message['task_params']['start']=message['task_params']['start']+self.etl_job.moex_limit*self.etl_job.moex_n_concurrent
                    message['task_params']['truncate']=False
                    self.logger.info(f"queue={header['topic']} step=1 message={result}")
                    header['topic']='tasks_topic'
                    self.__add_new_task(json.dumps(message),json.dumps(header))
                status_message=self.__status_message(new_status_message,header['topic'],current_status_state['created'],
                                                     n_iteration=current_status_state['n_iteration']+1)
            else:
                status_message=self.__status_message('iteration error',header['topic'],current_status_state['created'],
                                                     n_iteration=current_status_state['n_iteration']+1,error_message=result)
            
            self.__update_redis_task_status(redis_task_status,task_id,status_message)
            
        elif task_type=='check_upload_securities_dict':
            success_flg,result=self.etl_job.execute_etl_job(task_type,{})  
            #self.__update_redis_task_status(redis_task_status,task_id,result)
            self.logger.info(f"queue={header['topic']} step=3 message={result}")
            if success_flg:
                header['type']='copy_upload_securities_dict'
                self.__add_new_task(json.dumps({}),json.dumps(header))
                
        elif task_type in ('copy_upload_securities_dict','upload_moex_dicts'):
            success_flg,result=self.etl_job.execute_etl_job(task_type,{})  
            #self.__update_redis_task_status(redis_task_status,task_id,result)   
            self.logger.info(f"queue={header['topic']} step=4 message={result}")  

        elif task_type in ('update_securities_history'):
            #success_flg,result=self.etl_job.execute_etl_job(task_type,{})  
            #self.__update_redis_task_status(redis_task_status,task_id,result)  
            #task_type,message['task_params']                       
            self.logger.info(f"queue={header['topic']} step=1 message={message}") 
            start_date = datetime.strptime(message['start_date'], date_format) 
            end_date = datetime.strptime(message['end_date'], date_format) 
            delta = end_date - start_date
            days_list=[datetime.strftime(start_date + timedelta(days=i),date_format) for i in range(delta.days + 1)]
            task_params= {k:v for k,v in message.items() if k not in ('task_name','start_date','end_date')}
            success_flg,result=self.etl_job.execute_etl_job('update_securities_history',
                                                            {'task_params':task_params,'days_list':days_list})
            
            header['type']='upload_single_sr'
            header['topic']='tasks_topic'       
            self.__get_task_status(task_id)
            for _date in days_list:
                self.__add_new_task(json.dumps({'start':0,'oper_date':_date,'market':message['market'],'engine':message['engine']}),json.dumps(header))                           
            #self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('created subtasks',queue_name))
            self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('created_subtasks for each day',
                                                                                            header['topic'],
                                                                                            current_status_state['created']
                                                                                            ))            
            if success_flg:
                self.logger.info(f"queue={header['topic']} step=2 created tasks for interval [{message['start_date']},{message['end_date']}]") 
            else:
                self.logger.error(f"queue={header['topic']} step=2 message={result}") 
            #self.__add_new_task(json.dumps({}),json.dumps(header))
        elif task_type in ('upload_single_sr'):
            log_message=f"oper_date={message['oper_date']} engine={message['engine']} market={message['market']}"
            success_flg,result=self.etl_job.execute_etl_job(task_type,{**{'oper_date':datetime.strptime(message['oper_date'], date_format)},
                                                                       **{k:v for k,v in message.items() if k not in ('oper_date')}})             
            if success_flg:             
                end_flag=result['end_flag']
                if end_flag:
                    self.logger.info(f"queue={header['topic']} step=3 message:{log_message} status=complete")
                    
                    status_message=self.__status_message(f"day complete:{message['oper_date']}",header['topic'],current_status_state['created'],
                                                         n_iteration=current_status_state['n_iteration']+1)
                    self.__update_redis_task_status(redis_task_status,task_id,status_message)
                    
                else:
                   
                    message['start']=message['start']+self.etl_job.moex_limit*self.etl_job.moex_n_concurrent
                    message['truncate']=False                    
                    self.logger.info(f"queue={header['topic']} step=2 message:{log_message} status=next iteration")
                    header['topic']='tasks_topic'
                    self.__add_new_task(json.dumps(message),json.dumps(header))
            else:
                self.logger.error(f"queue={header['topic']} step=2 error_message:{result}")
        elif task_type=='upload_finam_dividends': 
            success_flg,result=self.etl_job.execute_etl_job(task_type,message['task_params'])   

            if success_flg:
                header['type']='upload_single_finam_dividends'
                header['topic']='tasks_topic'          
                for _secid in result:
                    self.__add_new_task(json.dumps({'task_params':{'secid':_secid}}),json.dumps(header))   
                                                                
                self.__update_redis_task_status(redis_task_status,task_id,self.__status_message('created_subtasks for each secid',
                                                                                            header['topic'],
                                                                                            current_status_state['created']
                                                                                            ))                       

                self.logger.info(f"queue={header['topic']} step=2 created tasks for {len(result)} securities") 
            else:
                self.logger.error(f"queue={header['topic']} step=1 message={result}") 
        elif task_type=='upload_single_finam_dividends': 
            self.__common_etl_task(task_type,message,header)     

    def __common_etl_task(self,task_type:str,message:dict,header:dict,success_handler:Callable=None,error_handler:Callable=None):
        success_flg,result=self.etl_job.execute_etl_job(task_type,message['task_params']) 
        base_success_message=f"queue={header['topic']} {task_type} completed,success_flg ={success_flg}"  
        base_error_message= f"queue={header['topic']} {task_type} failed, error_message={result}"
        if success_flg:
            self.logger.info(base_success_message)
            if success_handler is not None:
                success_handler(result)
        else:
            self.logger.error(base_error_message)
            if error_handler is not None:            
                error_handler(result) 

                      
    def __add_redis_dict(self,dict_name:str,key:str,value)->None:
        return redis_dict(redis_url,app_name).dict_add_key(dict_name=dict_name,key=key,value=value)
        
    def __update_redis_task_status(self,dict_name:str,key:str,new_value)->None:
        return redis_dict(redis_url,app_name).update_dict_value(dict_name=dict_name,key=key,new_value=new_value)
    
    def __get_task_status(self,task_id:str)->None:
        redis_value=redis_dict(redis_url,app_name).get_dict(redis_task_status,task_id)
        return json.loads(redis_value)
    
    def __get_task_info(self,task_id:str)->None:
        return json.loads(redis_dict(redis_url,app_name).get_dict(redis_task_original_message,task_id))    
    
    def __add_new_task(self,message:str,header:str)->None:       
        return redis_steams(redis_url).publish(self.redis_topics['tasks_topic']['name'],message,header)
    

