from requests import Session
from copy import deepcopy
from multiprocessing.dummy import Pool as ThreadPool

def moex_call(url:str,query_params:dict={},start:int=0,moex_root:str=None,moex_limit:int=None,moex_n_concurrent:int=None):
    
    query_params['limit']=moex_limit
    n_concurrent_requests=moex_n_concurrent
    params_list=[]
    for i in range(n_concurrent_requests):
        query_params['start']=start+moex_limit*i
        params_list.append({'url':url,'params':deepcopy(query_params)})
    next_start=query_params['start']+moex_limit
    with Session() as s:   
        pool = ThreadPool(n_concurrent_requests)     
        results=pool.map(lambda _params:__moex_parse_result(s,_params,moex_root),params_list)
        pool.close()
        pool.join()  
    data_list,end_flag=[],False
    for _result in results:
        data_list=data_list+_result['data']
        
        if _result['end_flag']:
            end_flag=True
        if _result['success'] is False:
            final_result={'success':False,'error_message':_result['error_message']}
            return final_result
    final_result={'success':True,'end_flag':end_flag,'data':data_list,'next_start':next_start}    
    return final_result   





def __moex_parse_result(session,call_params:dict,moex_root):
    res={'worker_params':call_params,'success':True,'end_flag':False}
    try:
        request_result=session.get(**call_params)
        res['data']=request_result.json()[moex_root]['data']
        if len(res['data'])==0:
            res['end_flag']=True
    except Exception as e:
        res['success'],res['error_message']=False,repr(e)
    return res
    



   