from datetime import date
#https://www.postman.com/tauc-2005/my-public/request/smrskju/
"""Типы ценных бумаг
https://iss.moex.com/iss/securitytypes.xml
Группы ценных бумаг
https://iss.moex.com/iss/securitygroups.xml
Доступные торговые системы.
https://iss.moex.com/iss/engines.xml
Рынки торговой системы.
https://iss.moex.com/iss/engines/:engine/markets.xml
Режимы торгов рынка.
https://iss.moex.com/iss/engines/:engine/markets/:market/boards.xml
Список сессий доступных в итогах торгов/ Только для фондового рынка!
https://iss.moex.com/iss/history/engines/:engine/markets/:market/sessions.xml
 """
class moex_api:
    __slots__ = ('base_url','output_format','date_format')
    def __init__(self,output_format:str='json')->None:
        self.base_url = f'https://iss.moex.com/iss/{{}}.{output_format}' 
        self.output_format=output_format
        self.date_format='%Y-%m-%d'
    
    def get_securities(self,is_trading:int=1)->dict:
        return {'url':self.base_url.format('securities'),
                'query_params':{'is_trading':is_trading}
                }    
        
    def engines_list(self)->dict:
        return {'url':self.base_url.format('engines')}
        
    def markets_list(self,engine:str='stock')->dict:
        return {'url':self.base_url.format(f'engines/{engine}/markets')}        
     
    def all_sec_history_per_day(self,trade_date:date,engine:str='stock',market='shares')->dict:
        url_ending=f'history/engines/{engine}/markets/{market}/securities'
        return {'url':self.base_url.format(url_ending),
                'query_params':{'trade_date':trade_date.strftime(self.date_format)}
                }    
            
    def sec_history_per_interval(self,sec_name:str,trade_begin:date,trade_end:date,engine:str='stock',market='shares')->dict:
        url_ending=f'history/engines/{engine}/markets/{market}/securities/{sec_name}'
        return {'url':self.base_url.format(url_ending),
                'query_params':{'start_date':trade_begin.strftime(self.date_format),'end_date':trade_end.strftime(self.date_format)}
                }        
     
     