from pydantic import BaseModel, ConfigDict,Field
from typing import Annotated
from uuid import uuid4
class BaseModelConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

class PublishTask(BaseModelConfig):
    id: str = Field(default_factory=lambda s:str(uuid4()))
    task: str
    task_params:dict
    
    
   


