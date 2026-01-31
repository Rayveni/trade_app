from fastapi import APIRouter, Form, File, UploadFile
# from loguru import logger
from fastapi.requests import Request
from backend.common_libs.redis_wrapper import redis_steams,redis_dict
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
#from .common_libs import *
import json
from .schemas import TopicParam,PublishMessage
#from ..core.upload import create_export_file
from os import getenv
from ast import literal_eval
router = APIRouter(prefix="/api/redis", tags=["REDIS"])
redis_url,app_name=getenv('redis_url'),getenv('app_name')

redis_topics=literal_eval(getenv('redis_topics'))

@router.get("/info")
async def external_url(request: Request):
    data = redis_steams(redis_url).info()
    json_data = jsonable_encoder(data)
    return JSONResponse(content=json_data)

@router.delete("/all")
async def delete_all_keys():
    redis_steams(redis_url).delete_all()
    return 'True'

@router.post("/publish")
async def publish_message(publish_info:PublishMessage):
    topic_dict = publish_info.dict()
    res=redis_steams(redis_url).publish(topic_dict['topic'],topic_dict['message'])
    return JSONResponse(content=jsonable_encoder(res))

@router.get("/consume/{topic}/{consumer_group}")
async def consume(topic:str,consumer_group:str):
    r_s = redis_steams(redis_url)
    redis_res=r_s.consume(topic,consumer_group)[0]
    commit_res=r_s.commit(topic,consumer_group,redis_res['message_id'])
    return JSONResponse(content=jsonable_encoder({'consume':redis_res,'commit':commit_res}))

@router.post("/create/topic")
async def create_topic(topic:TopicParam):
    topic_dict = topic.dict()
    res=redis_steams(redis_url).create_consumer_group(topic_dict['topic'],topic_dict['consumer_group'])
    return JSONResponse(content=jsonable_encoder(res))


@router.post("/create/topic/all")
async def create_topic():
    r_s=redis_steams(redis_url)
    res={}
    for k,v in redis_topics.items():
        res[k]= r_s.create_consumer_group(v['name'],v['consumer_group'])
    return JSONResponse(content=jsonable_encoder(res))


    
@router.get("/topic/info/all")
async def topic_infoall():
    r_s=redis_steams(redis_url)
    res={}
    for k,v in redis_topics.items():
        res[k]= r_s.consumer_group_info(v['name'])
    return JSONResponse(content=jsonable_encoder(res))

@router.get("/topic/info/{topic_id}")
async def topic_info(topic_id:str):
    res=redis_steams(redis_url).consumer_group_info(topic_id)
    return JSONResponse(content=jsonable_encoder(res))

@router.get("/keys/info/{dict_name}")
async def dict_info(dict_name:str,key:str=None):
    res=redis_dict(redis_url,app_name).get_dict(dict_name,key)
    return JSONResponse(content=jsonable_encoder(res))

