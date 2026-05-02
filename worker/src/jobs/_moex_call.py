from requests import Session

from multiprocessing.dummy import Pool as ThreadPool


def moex_call(

    list_query_params: list = [],
    next_start: int = None,
    moex_root: str = None,
    moex_n_concurrent: int = None,

): 

    with Session() as s:
        pool = ThreadPool(moex_n_concurrent)
        results = pool.map(
            lambda _params: __moex_parse_result(s, _params, moex_root), list_query_params
        )
        pool.close()
        pool.join()
    data_list, end_flag = [], False
    for _result in results:
        data_list = data_list + _result['data']

        if _result['end_flag']:
            end_flag = True
        if _result['success'] is False:
            final_result = {'success': False, 'error_message': _result['error_message']}
            return final_result
    final_result = {
        'success': True,
        'end_flag': end_flag,
        'data': data_list,
        'next_start': next_start,
    }
    return final_result


def __moex_parse_result(session, call_params: dict, moex_root:str):
    res = {'worker_params': call_params, 'success': True, 'end_flag': False}
    if 'output_constant' in call_params:
        output_constant=call_params.pop('output_constant')
    else:
        output_constant=None
    try:
        request_result = session.get(**call_params)
        res['data'] = request_result.json()[moex_root]['data']
        if len(res['data']) == 0 :
            res['end_flag'] = True
        if output_constant is not None:
            res['data']=[output_constant+_row for _row in res['data']]
            
    except Exception as e:
        res['success'], res['error_message'] = False, repr(e)
    return res
