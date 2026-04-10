from fastapi import APIRouter

from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from os import getenv
import json
from .schemas import TopicParam, PublishMessage

from sys import path

# appending a path
path.append('/app/common_libs')
from queue_interface import base_queue


router = APIRouter(prefix='/api/redis', tags=['REDIS'])
redis_url, app_name = getenv('redis_url'), getenv('app_name')

msg_broker = base_queue(driver='redis', connection_setting={'redis_url': redis_url})

with open('/app/topics_config.json', 'r') as file:
    redis_topics = json.load(file)


@router.get('/info')
async def redis_info(request: Request):
    data = msg_broker.conn_info()
    return JSONResponse(content=jsonable_encoder(data))


@router.delete('/all')
async def delete_all_keys():
    msg_broker.delete_all()
    return 'True'


@router.post('/publish')
async def publish_message(publish_info: PublishMessage):
    topic_dict = publish_info.dict()
    res = msg_broker.publish(**topic_dict)
    return JSONResponse(content=jsonable_encoder(res))


@router.get('/consume/{topic}/{consumer_group}')
async def consume(topic: str, consumer_group: str):
    redis_res = msg_broker.consume(topic, consumer_group, 1)
    commit_res = msg_broker.commit(topic, consumer_group, redis_res['message_id'])
    return JSONResponse(
        content=jsonable_encoder({'consume': redis_res, 'commit': commit_res})
    )


@router.post('/create/topic')
async def create_topic(topic: TopicParam):
    topic_dict = topic.dict()
    res = msg_broker.create_consumer_group(
        topic_dict['topic'], topic_dict['consumer_group']
    )
    return JSONResponse(content=jsonable_encoder(res))


@router.post('/create/topic/all')
async def create_default_topics(delete_all: bool = False):
    if delete_all:
        msg_broker.delete_all()
    res = {}
    for k, v in redis_topics.items():
        res[k] = msg_broker.create_consumer_group(k, v['consumer_group'])
    return JSONResponse(content=jsonable_encoder(res))


@router.get('/topic/info/all')
async def topic_infoall():
    res = {}
    for k, v in redis_topics.items():
        res[k] = msg_broker.consumer_group_info(k)
    return JSONResponse(content=jsonable_encoder(res))


@router.get('/topic/get_uncommited_messages/all')
async def all_get_uncommited_messages(count: int = 10):
    res = {}
    for k, v in redis_topics.items():
        res[k] = msg_broker.get_uncommited_messages(k, v['consumer_group'], count)
    return JSONResponse(content=jsonable_encoder(res))


@router.get('/topic/info/{topic_id}')
async def topic_info(topic_id: str):
    res = msg_broker.consumer_group_info(topic_id)
    return JSONResponse(content=jsonable_encoder(res))
