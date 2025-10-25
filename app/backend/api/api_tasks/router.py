from fastapi import APIRouter,Depends, Form, File, UploadFile
# from loguru import logger
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from .schemas import PublishTask
from ast import literal_eval
#from ..core.upload import create_export_file
from os import getenv

#from .schemas import UserTables
from backend.common_libs.redis_wrapper import redis_steams

router = APIRouter(prefix="/api/tasks", tags=["TASKS"])
redis_url,app_topic=getenv('redis_url'),literal_eval(getenv('redis_topics'))['app_topic']['name']




@router.post("/publish_task")
async def publish_task(task:PublishTask):
    res=redis_steams(redis_url).publish(app_topic,task.model_dump_json())
    return JSONResponse(content=jsonable_encoder({'id':task.id,
                                                  'response':res}))



