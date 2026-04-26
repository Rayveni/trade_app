from pydantic import BaseModel, ConfigDict
from uuid import uuid4


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