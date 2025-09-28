from pydantic import BaseModel, ConfigDict


class BaseModelConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class TopicParam(BaseModelConfig):
    topic: str
    consumer_group: str

class PublishMessage(BaseModelConfig):
    topic: str
    message: str