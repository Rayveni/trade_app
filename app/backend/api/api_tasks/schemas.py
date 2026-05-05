from pydantic import BaseModel, ConfigDict
from uuid import uuid4

from datetime import date,datetime
from os import getenv

class BaseModelConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class MessageBody(BaseModelConfig):
    task_name: str
    task_params: dict


class MessageHeader(BaseModelConfig):
    id: str = str(uuid4())  
    task_name: str = 'default'


class UserRegistration(BaseModel):
    username: str
    password: str
    email: str

class UploadSecuritiesDictionary(BaseModel):
    start: int=0
    truncate: bool = True
    
class RetryTask(BaseModelConfig):
    task_id: str 
   
class UploadMoexDictionaries(BaseModelConfig):
    truncate: bool = False
    
class SecuritiesHistory(BaseModelConfig):
    engine:str ='stock'
    market:str ='shares'
    start_date:date=date(int(getenv('start_trade_year')),1,1)
    end_date:date=datetime.now().date()
    
class FinamSecPrepare(BaseModelConfig):    
    truncate: bool = False
    is_traded: int = 1
    group: str = 'stock_shares'
    primary_boardid: str = 'TQBR'

