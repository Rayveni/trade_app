from pydantic import BaseModel, ConfigDict,Field
from uuid import uuid4
from datetime import date,datetime
from os import getenv

class BaseModelConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

class MessageBody(BaseModelConfig):
    task_name: str
    task_params:dict

        
class MessageHeader(BaseModelConfig):
    id: str = str(uuid4())#Field(default_factory=uuid4)  # lambda s:str(uuid4())
    type: str='default'

            
class SecuritiesHistory(BaseModelConfig):
    task_name: str='update_securities_history'
    engine:str ='stock'
    market:str ='shares'
    start_date:date=date(int(getenv('start_trade_year')),1,1)
    end_date:date=datetime.now().date()
    

   


