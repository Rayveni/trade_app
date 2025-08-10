from pydantic import BaseModel, ConfigDict,StringConstraints
from typing import Annotated

class BaseModelConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class UserTables(BaseModelConfig):
    return_type: Annotated[str, StringConstraints(max_length=7)]  = 'json'
    
    
   


