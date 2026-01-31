from backend.common_libs.moex_api import moex_api
from etl.moex_etl import get_unbound_moex_query,single_moex_query
from backend.common_libs.moex_api import moex_api
from backend.common_libs.selenium_wrapper import selenium_wrapper
import base64
import json
import traceback
class etl_jobs:
    __slots__ = ('pg_conn','moex_api','moex_limit','moex_n_concurrent','pg_table_params','sql_path','selenium_options','finam_urls')    
    def __init__(self,pg_conn,moex_limit:int,moex_n_concurrent:int)->None:    
        self.pg_conn=pg_conn
        self.moex_api=moex_api()
        self.moex_limit=moex_limit
        self.moex_n_concurrent=moex_n_concurrent
        self.pg_table_params={'etl_log':{'table_name':'etl_log',
                                         'pk_columns':['table_name','start_param', 'oper_date'],
                                         'columns':['table_name','status_flg','start_param','oper_date','query','error_message'],
                                         },
                              'temp_securities_dict':{'table_name':'temp_securities_dict',
                                         'pk_columns':['secid'],
                                         'columns':['secid','shortname','regnumber',"name",'isin','is_traded','emitent_id',
                                                    'emitent_title','emitent_inn','emitent_okpo',"type",'"group"','primary_boardid',
                                                    'marketprice_boardid'],'moex_root':'securities'},
                              'securities_dict':{'table_name':'securities_dict'},
                              'securitytypes':{'table_name':'securitytypes','columns':['id','name','title'],
                                               'url':self.moex_api.get_securitytypes()['url'],'pk_columns':[],'moex_root':'securitytypes'},
                               'securitygroups':{'table_name':'securitygroups','columns':['id','name','title','is_hidden'],
                                               'url':self.moex_api.get_securitygroups()['url'],'pk_columns':[],'moex_root':'securitygroups'},
                                'engines':{'table_name':'engines','columns':['id','name','title'],
                                               'url':self.moex_api.get_engines()['url'],'pk_columns':[],'moex_root':'engines'},    
                                'markets':{'table_name':'markets','columns':['engine','id','name','title'],
                                               'url':lambda engine:self.moex_api.get_markets(engine)['url'],'pk_columns':[],'moex_root':'markets'},    
                                'boards':{'table_name':'boards','columns':['engine','market','id','board_group_id' ,'boardid' ,'title' ,'is_traded'],
                                               'url':lambda engine,markets:self.moex_api.get_boards(engine,markets)['url'],'pk_columns':[],'moex_root':'boards'},                                                                                  
                              'securities_hist':{'table_name':'securities_hist',
                                         'pk_columns':['tradedate' ,'secid' ,'boardid'],
                                         'columns':['boardid','tradedate','shortname','secid','numtrades','value','open','low','high','legalcloseprice','waprice',
                                                    'close','volume','marketprice2','marketprice3','admittedquote','mp2valtrd','marketprice3tradesvalue','admittedvalue',
                                                    'waval','tradingsession','currencyid','trendclspr','trade_session_date'],'moex_root':'history'
                                         }
                              }
        self.selenium_options=['--no-sandbox',
                               '--headless=new',
                               'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
                               ] 
        self.finam_urls={'moex_sec_dividends':r'https://www.finam.ru/quote/moex/{}/dividends/'}
        
        
        
        self.sql_path={'delete':'etl/sql/delete_query.sql',
                       'check_delta_query':'etl/sql/check_delta_query.sql',
                       'check_errors_query':'etl/sql/check_errors_query.sql',
                       'copy_securities_dict':'etl/sql/copy_securities_dict.sql',
                       'finam_data_divs':'etl/sql/finam_data_divs.sql',
                       'finam_nonqual_secid':'etl/sql/finam_nonqual_secid.sql',
                       'finam_divs':'etl/sql/update_finam_divs.sql',
                       'update_etl_log_success':'etl/sql/update_etl_log.sql'}     
              
    def execute_etl_job(self,job_name:str,job_params:dict)->tuple:
        try:
            if  job_name=='upload_securities_dict':
                res= (True,self.__upload_securities_dict(job_params))
            elif job_name=='check_upload_securities_dict':
                res= self.__check_upload_securities_dict()
            elif job_name=='copy_upload_securities_dict':
                res=(True,self.__copy_upload_securities_dict())
            elif job_name=='upload_moex_dicts':
                res=(True,self.__upload_moex_dicts())
            elif job_name=='update_securities_history':
                res=(True,self.__update_securities_history(job_params))   
            elif job_name=='upload_single_sr':
                res= (True,self.__upload_single_sr(job_params))       
            elif job_name=='upload_finam_dividends':
                res= (True,self.__upload_finam_dividends()) 
            elif job_name=='upload_single_finam_dividends':
                res= self.__upload_single_finam_dividends(job_params)                
        except Exception as e:
            res=(False,traceback.format_exc())
        return res
    
    def __upload_single_finam_dividends(self,job_params)->tuple:   
        sl=selenium_wrapper(self.selenium_options)
        secid=job_params['secid']
        page_source=sl.get_page_source( self.finam_urls['moex_sec_dividends'].format(secid))
        
        if page_source[0]:
            page_source_encoded= base64.b64encode(page_source[1].encode('utf-8'))
            sql_query=self.__prepare_sql_from_file(self.sql_path['finam_divs'],secid=secid)
            res=(True,self.pg_conn._execute(sql_query,(page_source_encoded,page_source_encoded)))
            sql_query=self.__prepare_sql_from_file(self.sql_path['update_etl_log_success'],secid=secid,table='finam_data_divs')
            self.pg_conn._execute(sql_query)
            return res
        else:
            return page_source
    
    def __upload_finam_dividends(self)->list:
        delete_query=self.__prepare_sql_from_file(self.sql_path['delete'],
                                                  table=self.pg_table_params['etl_log']['table_name'],
                                                  condition=f"table_name='finam_data_divs'")
        self.pg_conn._execute(delete_query)         
        
        
        query=self.__prepare_sql_from_file(self.sql_path['finam_data_divs'])
        self.pg_conn._execute(query)
        query2=self.__prepare_sql_from_file(self.sql_path['finam_nonqual_secid'])
        return  [el[0] for el in  self.pg_conn.fetch_all(query2,'list')[1:]]#[:2]
        
    def __upload_single_sr(self,job_params):
        _table_name=self.pg_table_params['securities_hist']['table_name']
        if job_params.get('truncate',False):
            self.pg_conn.truncate(_table_name)
            
            
        request_info=self.moex_api.all_sec_history_per_day(trade_date=job_params['oper_date'],engine=job_params['engine'],market=job_params['market'])  
        data_list,status_list,end_flag=get_unbound_moex_query(self.moex_limit
                                                             ,self.moex_n_concurrent
                                                             ).get_all_data(request_info['url'],
                                                                            self.pg_table_params['securities_hist']['moex_root'],
                                                                            job_params['start'],
                                                                            request_info['query_params']
                                                                            )   
                
        self.__upsert_many('etl_log',
                           list(map(lambda row:[_table_name,
                                                row['success'],
                                                row['worker_params']['params']['start'],
                                                row['worker_params']['params']['date'],
                                                json.dumps(row['worker_params']),
                                                row['error_message']
                                                ],
                                                status_list
                                    ))
                           )
        self.__upsert_many('securities_hist',data_list)
        return {'end_flag':end_flag}



    def __update_securities_history(self,job_params:dict)->str:
        _table_name=self.pg_table_params['securities_hist']['table_name']
        task_params,days_list=job_params['task_params'],job_params['days_list']
        
        self.__upsert_many('etl_log',
                           list(map(lambda oper_day:[_table_name,
                                                False,
                                                oper_day,
                                                json.dumps({**task_params,**{'start_date':oper_day,'end_date':oper_day}})
                                                #row['error_message']
                                                ],
                                                days_list
                                    )),
                           ['start_param','error_message']
                           )
        return f"To etl_log added interval {days_list[0]} - {days_list[-1]}"
        
    
                                         
            
    def __upload_moex_dicts(self):  
        dict_list=['securitytypes','securitygroups','engines']
        for _dict in dict_list:
            _table=self.pg_table_params[_dict]  
            _data=single_moex_query().execute_request(_table['url'])[_table['moex_root']]['data']
            self.pg_conn.truncate(_table['table_name'])
            self.__upsert_many(_dict,_data)
         
        _table=self.pg_table_params['markets']
        _markets_data=[]
        for _row in _data:
            _engine=_row[1]
            _data_chunk=single_moex_query().execute_request(_table['url'](_engine))[_table['moex_root']]['data']
            _markets_data=_markets_data+[[_engine]+el for el in _data_chunk ]
        self.pg_conn.truncate(_table['table_name'])
        self.__upsert_many('markets',_markets_data)                          
        dict_list.append('markets') 

        _table=self.pg_table_params['boards']
        _boards_data=[]
        for _row in _markets_data:
            _engine,_market=_row[0],_row[2]
            _data_chunk=single_moex_query().execute_request(_table['url'](_engine,_market))[_table['moex_root']]['data']
            _boards_data=_boards_data+[[_engine,_market]+el for el in _data_chunk ]
        self.pg_conn.truncate(_table['table_name'])
        self.__upsert_many('boards',_boards_data)                          
        dict_list.append('boards')        
        
        return f"updated: {','.join(dict_list)}"
    
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
            
            delete_query=self.__prepare_sql_from_file(self.sql_path['delete'],
                                                        table=self.pg_table_params['etl_log']['table_name'],
                                                        condition=f"table_name='{_table_name}'")
            self.pg_conn._execute(delete_query) 
            
        request_info=self.moex_api.get_securities()  
        data_list,status_list,end_flag=get_unbound_moex_query(self.moex_limit
                                                             ,self.moex_n_concurrent
                                                             ).get_all_data(request_info['url'],
                                                                            self.pg_table_params['temp_securities_dict']['moex_root'],
                                                                            job_params['start'],
                                                                            request_info['query_params']
                                                                            )   
                    
        self.__upsert_many('etl_log',
                           list(map(lambda row:[_table_name,
                                                row['success'],
                                                row['worker_params']['params']['start'],
                                                None,
                                                json.dumps(row['worker_params']),
                                                row['error_message']
                                                ],
                                                status_list
                                    ))
                           )
        self.__upsert_many('temp_securities_dict',data_list)
        return {'end_flag':end_flag}


        
    def __upsert_many(self,table_name:str,insert_arr:list,ignore_columns:list=None)->None:
        table_params=self.pg_table_params[table_name]
        columns=table_params['columns']
        if ignore_columns is not None:
            columns=[_column for _column in columns if _column not in ignore_columns]
        self.pg_conn.insert_many(table_params['table_name'],
                columns,
                insert_arr,
                conflict=table_params['pk_columns'])  
    
    def __prepare_sql_from_file(self,sql_path:str,**kwargs)->None:
        with open(sql_path, 'r', encoding='utf-8') as file:
            query = file.read() # Read the entire file content into a single string
            
        return query.format(**kwargs)  
        
        
        
        