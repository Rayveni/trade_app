from pathlib import Path
from sys import path
from copy import deepcopy

path.append('/worker/common_libs')
from moex_api import moex_api
from os import getenv
from ._moex_call import moex_call
from json import load


class Jobs:
    __slots__ = (
        'logger',
        'msg_broker',
        'task_db_driver',
        'main_db_driver',
        'sql_dict',
        'moex_api',
        'moex_params',
        'task_settings',
    )

    def __init__(self, msg_broker, task_db_driver, main_db_driver, logger):
        self.logger = logger
        self.msg_broker = msg_broker
        self.task_db_driver = task_db_driver
        self.main_db_driver = main_db_driver

        self.sql_dict = {}
        for file_path in Path('/worker/worker/src/jobs/sql/fin_db').rglob('*.sql'):
            with open(file_path, 'r') as f:
                self.sql_dict[str(file_path.stem)] = f.read()
        self.moex_api = moex_api()
        self.moex_params = {
            'moex_limit': int(getenv('moex_limit')),
            'moex_n_concurrent': int(getenv('moex_n_concurrent')),
        }
        with open('/worker/worker/src/jobs/task_settings.json', 'r') as file:
            self.task_settings = load(file)

    def __delete_table(self, table_name: str, conditions: str = 'true') -> None:
        sql = self.sql_dict['common_delete'].format(
            table=table_name, condition=conditions
        )
        self.main_db_driver.execute(sql)

    def first_task(self, msg_id):
        self.logger.info(
            f'~~~~~////////////////////////////////////////////~~~{msg_id}~~~~~~~~~~~~'
        )
        # получить ссылку
        request_info = self.moex_api.get_securities()
        moex_root = 'securities'
        # получить данные
        res = moex_call(
            url=request_info.get('url'),
            query_params=request_info.get('query_params', {}),
            moex_root=moex_root,
            moex_limit=self.moex_params['moex_limit'],
            moex_n_concurrent=self.moex_params['moex_n_concurrent'],
        )

        self.logger.info(
            f'~~~~~////////////////////////////////////////////~~~{res["next_start"]}~~~~~~~~~~~~'
        )


    def task_pipline(self, message: dict) -> dict:
        self.logger.info(message)
        settings = self.task_settings.get(message['header']['type'])
        if settings is None:
            step_output = {
                'success': False,
                'error_message': f"No task settings found for {message['header']['type']}",
            }
           
        elif settings['type']=='moex':
            if message['message'].get('truncate','false') =='true':
                truncate_flg=True
            
            else:
                truncate_flg=False
            step_output = {}
            saved_data=[]
            for step in settings['pipeline']:              
                ((key, value),) = step.items()
                if key == 'moex_method':
                    step_output = getattr(self.moex_api, value)()
                elif key == 'add_parameter':
                    step_output = {**step_output, **value}
                elif key == 'execute_moex_method':
                    execute_type=value['execute_type']
                    url=step_output.get('url')
                    query_params=step_output.get('query_params', {})
                    moex_root=step_output.get('moex_root')
                    params_list ,next_start = [] ,None                    
                    start=int(message['message'].get('start',0))
                    n_concurrent=deepcopy(self.moex_params['moex_n_concurrent'])

                    if execute_type=='iteration':
                        for i in range(n_concurrent):
                            query_params['start'] = start + self.moex_params['moex_limit'] * i
                            params_list.append({'url': url, 'params': deepcopy(query_params)})
                        next_start = query_params['start'] + self.moex_params['moex_limit']

                    elif execute_type=='single':
                        n_concurrent=1
                        params_list.append({'url': url, 'params': deepcopy(query_params)})
                    
                    elif execute_type=='parallel_prev_step':
                        for _row in saved_data:
                            url_params={k:_row[v] for k,v in value['index_mapper'].items()}
                            params_list.append({'url': url.format(**url_params), 
                                                'params': query_params,
                                                'output_constant':list(url_params.values())})
                        saved_data=[]
                        
                    else:
                        step_output = {
                            'success': False,
                            'error_message': f"Unknown pipleline value {key, value}"}
                
 
                    step_output = moex_call(
                        list_query_params=params_list,
                        next_start=next_start,
                        moex_root=moex_root,
                        moex_n_concurrent=n_concurrent,

                    )
                    
                    if step_output['success'] is False:
                        break
                    if execute_type!='iteration':
                        step_output['end_flag']=True
                    if int(value.get('save_data',0))==1:
                        saved_data=deepcopy(step_output['data'])
                elif key == 'sql':
                    if value['query'] == 'truncate' and truncate_flg is True:
                        self.main_db_driver.truncate(value['table'])
                    elif value['query'] == 'insert_many':
                        self.main_db_driver.insert_many(
                            table=value['table'],
                            columns=value['columns'],
                            values_list=step_output['data'],
                            conflict=value['pk_columns'],
                        )

            step_output.pop('data')
            end_flg = step_output.get('end_flag', True)
            if end_flg is False:
                message['message']['start']=step_output['next_start']
                if truncate_flg:
                    message['message']['truncate']='false'
                step_output['subtasks']=[message]
        else:
            pass
        
        return step_output
