from backend.common_libs.redis_wrapper import redis_steams,redis_dict
from backend.common_libs.moex_api import moex_api
from backend.common_libs.pg_wrapper import pg_wrapper
from moex_etl import get_unbound_moex_query
from os import getenv
from ast import literal_eval
import json
from datetime import datetime

#redis_topics=literal_eval(getenv('redis_topics'))
redis_url,app_name=getenv('redis_url'),getenv('app_name')
redis_task_status,redis_task_original_message=getenv('redis_task_status'),getenv('redis_task_original_message')

moex_api_instance=moex_api()

moex_limit,moex_n_concurrent=getenv('moex_limit'),getenv('moex_n_concurrent')



class redis_jobs:
    __slots__ = ('redis_conn','redis_topics','pg_conn','pg_table_params')
    def __init__(self)->None:
        self.redis_conn = redis_steams(getenv('redis_url'))
        self.redis_topics=literal_eval(getenv('redis_topics'))
        self.pg_conn=pg_wrapper(getenv('pg_url'))
        self.pg_table_params={'etl_log':{'table_name':'etl_log',
                                         'pk_columns':('table_name','start_param', 'oper_date'),
                                         'columns':('table_name','status_flg','start_param','query','error_message')},
                              'temp_securities_dict':{'table_name':'temp_securities_dict',
                                         'pk_columns':('secid'),
                                         'columns':('secid','shortname','regnumber',"name",'isin','is_traded','emitent_id','emitent_title','emitent_inn','emitent_okpo',"type","group",'primary_boardid','marketprice_boardid')}
                              }
        
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
        status_message={'status':'new_task',
                        'queue_name':queue_name,
                        'created':now_datetime,
                        'updated':now_datetime
                        }   
        
        
        message_header=json.loads(queue_message['header'])
        task_id=message_header['id']
        #message_id=queue_message['message_id']     
        if queue_name=='app_topic':
            
            print('~~~~~~~~~~~~~~~~~0000000000~~~~~~~~~~~~~~~~~~~~')
            print(queue_message)
            print('~~~~~~~~~~~~~~~~~0000000000~~~~~~~~~~~~~~~~~~~~')
            redis_dict(redis_url,app_name).dict_add_key(dict_name=redis_task_status,key=task_id,value=json.dumps(status_message))
            redis_dict(redis_url,app_name).dict_add_key(dict_name=redis_task_original_message,key=task_id,value=queue_message['message'])
    
    
            res=redis_steams(redis_url).publish(self.redis_topics['tasks_topic']['name'],{'topic':queue_name,'key':task_id})
        else:
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print(queue_message)
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


        return 'success'
    
    def _update_securities_dict(self)->None:
        request_info=moex_api_instance.get_securities()
        data_list,status_list,end_flag=get_unbound_moex_query(moex_limit
                                                             ,moex_n_concurrent
                                                             ).get_all_data(request_info['url'],
                                                                            0,
                                                                            request_info['query_params']
                                                                            )
        table_params=self.pg_table_params['etl_log']
        try:
            self.pg_conn.insert_many(table_params['table_name'],
                table_params['columns'],
                list(map(lambda row:[table_params['table_name'],
                                        row['success'],
                                        row['worker_params']['params']['start'],
                                        json.dumps(row['worker_params']),
                                        row['error_message']]
                            ,
                            status_list)),
                conflict=table_params['pk_columns'])
            
            self.__upsert_many('etl_log',
                               list(map(lambda row:[table_params['table_name'],
                                                    row['success'],
                                                    row['worker_params']['params']['start'],
                                                    json.dumps(row['worker_params']),
                                                    row['error_message']
                                                    ],
                                                    status_list
                                        ))
                               )
            self.__upsert_many('temp_securities_dict',data_list)
        except Exception as e:
            pass
    def __upsert_many(self,table_name:str,insert_arr:list)->None:
        table_params=self.pg_table_params[table_name]
        self.pg_conn.insert_many(table_params['table_name'],
                table_params['columns'],
                insert_arr,
                conflict=table_params['pk_columns'])        
        
  

    

#read_tasks_queue