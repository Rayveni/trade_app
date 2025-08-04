from pydantic import BaseModel, ConfigDict


class BaseModelConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class CreateTopic(BaseModelConfig):
    topic: str
    consumer_group: str
