from backend.common_libs.moex_api import moex_api
from etl.moex_etl import get_unbound_moex_query
from backend.common_libs.moex_api import moex_api
import json
import traceback
class etl_jobs:
    __slots__ = ('pg_conn','moex_api','moex_limit','moex_n_concurrent','pg_table_params','sql_path')    
    def __init__(self,pg_conn,moex_limit:int,moex_n_concurrent:int)->None:    
        self.pg_conn=pg_conn
        self.moex_api=moex_api()
        self.moex_limit=moex_limit
        self.moex_n_concurrent=moex_n_concurrent
        self.pg_table_params={'etl_log':{'table_name':'etl_log',
                                         'pk_columns':['table_name','start_param', 'oper_date'],
                                         'columns':['table_name','status_flg','start_param','query','error_message']},
                              'temp_securities_dict':{'table_name':'temp_securities_dict',
                                         'pk_columns':['secid'],
                                         'columns':['secid','shortname','regnumber',"name",'isin','is_traded','emitent_id',
                                                    'emitent_title','emitent_inn','emitent_okpo',"type","group",'primary_boardid',
                                                    'marketprice_boardid']},
                              'securities_dict':{'table_name':'securities_dict'}
                              }   
        self.sql_path={'delete':'etl/sql/delete_query.sql',
                       'check_delta_query':'etl/sql/check_delta_query.sql',
                       'check_errors_query':'etl/sql/check_errors_query.sql',
                       'copy_securities_dict':'etl/sql/copy_securities_dict.sql'}     
              
    def execute_etl_job(self,job_name:str,job_params:dict)->tuple:
        try:
            if  job_name=='upload_securities_dict':
                res= (True,self.__upload_securities_dict(job_params))
            elif job_name=='check_upload_securities_dict':
                res= self.__check_upload_securities_dict()
            elif job_name=='copy_upload_securities_dict':
                res=(True,self.__copy_upload_securities_dict())
            
        except Exception as e:
            res=(False,traceback.format_exc())
        return res
    def __copy_upload_securities_dict(self):
        query=self.__prepare_sql_from_file(self.sql_path['copy_securities_dict'],
                                                        target_table=self.pg_table_params['securities_dict']['table_name']  ,
                                                        source_table=self.pg_table_params['temp_securities_dict']['table_name'])
        self.pg_conn._execute(query)         
        
        
        return 'securities_dict updated'
    def __check_upload_securities_dict(self):
        temp_sec_name=self.pg_table_params['temp_securities_dict']['table_name']
        check_errors_query=self.__prepare_sql_from_file(self.sql_path['check_errors_query'],
                                                        table=temp_sec_name) 
        result_check_errors_query=self.pg_conn.fetch_all(check_errors_query)
        
        if result_check_errors_query!=[]:
            return (False,f'result_check_errors_query returns non empty result set')  
        check_delta_query=self.__prepare_sql_from_file(self.sql_path['check_delta_query'],
                                                        table=temp_sec_name)    
        result_check_delta_query=self.pg_conn.fetch_all(check_delta_query)
        if result_check_delta_query[0]['max_value']!=self.moex_limit:
            return (False,f'result_check_delta_query={result_check_delta_query} not matched with limit ={self.moex_limit}')
        
        return (True,'All checks are passed')
            
    def __upload_securities_dict(self,job_params):
        _table_name=self.pg_table_params['temp_securities_dict']['table_name']
        if job_params['truncate']:
            self.pg_conn.truncate(_table_name)
            
            truncate_query=self.__prepare_sql_from_file(self.sql_path['delete'],
                                                        table=self.pg_table_params['etl_log']['table_name'],
                                                        condition=f"table_name='{_table_name}'")
            self.pg_conn._execute(truncate_query) 
            
        request_info=self.moex_api.get_securities()  
        data_list,status_list,end_flag=get_unbound_moex_query(self.moex_limit
                                                             ,self.moex_n_concurrent
                                                             ).get_all_data(request_info['url'],
                                                                            job_params['start'],
                                                                            request_info['query_params']
                                                                            )   
                    
        self.__upsert_many('etl_log',
                           list(map(lambda row:[_table_name,
                                                row['success'],
                                                row['worker_params']['params']['start'],
                                                json.dumps(row['worker_params']),
                                                row['error_message']
                                                ],
                                                status_list
                                    ))
                           )
        self.__upsert_many('temp_securities_dict',data_list)
        return {'end_flag':end_flag}


        
    def __upsert_many(self,table_name:str,insert_arr:list)->None:
        table_params=self.pg_table_params[table_name]
        self.pg_conn.insert_many(table_params['table_name'],
                table_params['columns'],
                insert_arr,
                conflict=table_params['pk_columns'])  
    
    def __prepare_sql_from_file(self,sql_path:str,**kwargs)->None:
        with open(sql_path, 'r', encoding='utf-8') as file:
            query = file.read() # Read the entire file content into a single string
            
        return query.format(**kwargs)  
        
        
        
        