from fastapi import APIRouter,Depends, Form, File, UploadFile
# from loguru import logger
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from .schemas import MessageBody,MessageHeader,SecuritiesHistory
from ast import literal_eval
#from ..core.upload import create_export_file
from os import getenv

#from .schemas import UserTables
from backend.common_libs.redis_wrapper import redis_steams

router = APIRouter(prefix="/api/tasks", tags=["TASKS"])
redis_url,app_topic=getenv('redis_url'),literal_eval(getenv('redis_topics'))['app_topic']['name']




@router.post("/publish_task")
async def publish_task(message:MessageBody,header:MessageHeader):
    res=redis_steams(redis_url).publish(app_topic,message.model_dump_json(),header.model_dump_json())
    return JSONResponse(content=jsonable_encoder({'response':res}))

@router.post("/publish_task/securities_dict")
async def upload_securities_dict(start:int=0,truncate:bool=True):
    message=MessageBody(task_name='upload_securities_dict',task_params={'start':start,'truncate':truncate})
    header=MessageHeader(type='upload_securities_dict')
    res=redis_steams(redis_url).publish(app_topic,message.model_dump_json(),header.model_dump_json())
    return JSONResponse(content=jsonable_encoder({'response':res}))

@router.post("/publish_task/upload_moex_dicts")
async def upload_moex_dicts():
    message=MessageBody(task_name='upload_moex_dicts',task_params={})
    header=MessageHeader(type='upload_moex_dicts')
    res=redis_steams(redis_url).publish(app_topic,message.model_dump_json(),header.model_dump_json())
    return JSONResponse(content=jsonable_encoder({'response':res}))

@router.post("/publish_task/update_securities_history")
async def update_securities_history(message:SecuritiesHistory):
    header=MessageHeader(type='update_securities_history')
    res=redis_steams(redis_url).publish(app_topic,message.model_dump_json(),header.model_dump_json())
    return JSONResponse(content=jsonable_encoder({'response':res}))