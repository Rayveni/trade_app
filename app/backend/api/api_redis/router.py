from fastapi import APIRouter, Form, File, UploadFile
# from loguru import logger
from fastapi.requests import Request
from backend.common_libs.redis_wrapper import redis_steams
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
#from .common_libs import *
import json
from .schemas import CreateTopic
#from ..core.upload import create_export_file
from os import getenv
from ast import literal_eval
router = APIRouter(prefix="/api/redis", tags=["API","REDIS"])
redis_url=getenv('redis_url')

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

@router.post("/create/topic")
async def create_topic(topic:CreateTopic):
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


    
@router.get("/create/topic/info/all")
async def topic_infoall():
    r_s=redis_steams(redis_url)
    res={}
    for k,v in redis_topics.items():
        res[k]= r_s.consumer_group_info(v['name'])
    return JSONResponse(content=jsonable_encoder(res))

@router.get("/create/topic/info/{topic_id}")
async def topic_info(topic_id:str):
    res=redis_steams(redis_url).consumer_group_info(topic_id)
    return JSONResponse(content=jsonable_encoder(res))



