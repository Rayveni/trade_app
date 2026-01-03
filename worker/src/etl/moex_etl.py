
from multiprocessing.dummy import Pool as ThreadPool
from requests import get as requests_get
from copy import deepcopy

class get_unbound_moex_query:
    __slots__ = ('limit_per_page','n_concurrent_requests','moex_root')
    
    def __init__(self,limit_per_page:int,n_concurrent_requests:int)->None:    
        self.limit_per_page=limit_per_page
        self.n_concurrent_requests=n_concurrent_requests
        
    def get_all_data(self,url:str,moex_root:str,start:int=0,query_params:dict={}):
        self.moex_root=moex_root
        requests_result=self.execute_requests(url,start,query_params)
        return self._process_results(requests_result)
        #next()
         
        
    def execute_requests(self,url:str,start:int,query_params:dict):
        query_params['limit']=self.limit_per_page
        params_list=[]
        for i in range(self.n_concurrent_requests):
            query_params['start']=start+self.limit_per_page*i
            params_list.append({'url':url,'params':deepcopy(query_params)})
        pool = ThreadPool(self.n_concurrent_requests)     
        results=pool.map(self._worker,params_list)
        pool.close()
        pool.join()  
        return results
    
    def _worker(self,worker_params)->dict: 
        res={'worker_params':worker_params,'success':True,'end_flag':False}
        try:
            request_result=requests_get(**worker_params)
            res['data']=request_result.json()[self.moex_root]['data']
            if len(res['data'])==0:
                res['end_flag']=True
        except Exception as e:
            res['success'],res['error_message']=False,repr(e)
        return res
    
    def _process_results(self,r_results:list)->tuple: 
        data_list,status_list,end_flag=[],[],False
        for _row in r_results:
            data_list=data_list+_row['data']
            if _row['end_flag']:
                end_flag=True
            status_list=status_list+[{'success':_row['success'],'worker_params':_row['worker_params'],'error_message':_row.get('error_message',None)}]
        return data_list,status_list,end_flag    

class single_moex_query:
    def execute_request(self,url:str)->list:
        res=requests_get(url)
        return res.json()