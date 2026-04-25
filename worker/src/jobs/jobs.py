
from pathlib import Path
from sys import path
path.append('/worker/common_libs')
from moex_api import moex_api
from os import getenv
from requests import Session
from copy import deepcopy

from multiprocessing.dummy import Pool as ThreadPool

from ._moex_call import moex_call
class Jobs:
    __slots__ = ('logger','msg_broker','task_db_driver','main_db_driver','sql_dict','moex_api','moex_params')
    def __init__(self,msg_broker,task_db_driver,main_db_driver,logger):
        self.logger = logger
        self.msg_broker = msg_broker
        self.task_db_driver = task_db_driver
        self.main_db_driver=main_db_driver
        
        self.sql_dict={}
        for file_path in Path('/worker/worker/src/jobs/sql/fin_db').rglob('*.sql'):
            with open(file_path, 'r') as f:
                self.sql_dict[str(file_path.stem)] = f.read()
        self.moex_api=moex_api()
        self.moex_params={'moex_limit':int(getenv('moex_limit')),'moex_n_concurrent':int(getenv('moex_n_concurrent'))}




    def __delete_table(self,table_name:str,conditions:str='true') -> None:
        sql=self.sql_dict['common_delete'].format(table=table_name,condition=conditions)
        self.main_db_driver.execute(sql)  
    
    def first_task(self,msg_id):
        self.logger.info(f'~~~~~////////////////////////////////////////////~~~{msg_id}~~~~~~~~~~~~')
        request_info=self.moex_api.get_securities()  
        moex_root='securities'
        self.__moex_call(url=request_info.get('url'),query_params=request_info.get('query_params',{}),moex_root=moex_root)
   
    def __moex_parse_result(self,session,call_params:dict,moex_root):
        res={'worker_params':call_params,'success':True,'end_flag':False}
        try:
            request_result=session.get(**call_params)
            res['data']=request_result.json()[moex_root]['data']
            if len(res['data'])==0:
                res['end_flag']=True
        except Exception as e:
            res['success'],res['error_message']=False,repr(e)
        return res
        
    def __moex_call(self,url:str,query_params:dict={},start:int=0,moex_root:str=None):
        
        query_params['limit']=self.moex_params['moex_limit']
        n_concurrent_requests=self.moex_params['moex_n_concurrent']
        params_list=[]
        for i in range(n_concurrent_requests):
            query_params['start']=start+self.moex_params['moex_limit']*i
            params_list.append({'url':url,'params':deepcopy(query_params)})
        next_start=query_params['start']+self.moex_params['moex_limit']
        with Session() as s:   
            pool = ThreadPool(n_concurrent_requests)     
            results=pool.map(lambda _params:self.__moex_parse_result(s,_params,moex_root),params_list)
            pool.close()
            pool.join()  
        data_list,end_flag=[],False
        for _result in results:
            data_list=data_list+_result['data']
            self.logger.info(query_params['start'])
            if _result['end_flag']:
                end_flag=True
            if _result['success'] is False:
                final_result={'success':False,'error_message':_result['error_message']}
                return final_result
        final_result={'success':True,'end_flag':end_flag,'data':data_list,'next_start':next_start}    
        return final_result   
    
    def upload_securities_dict(self):
        
        if False:
            self.main_db_driver.truncate(_table_name)

            
        request_info=self.moex_api.get_securities()  
       