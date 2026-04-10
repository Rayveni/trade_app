from fastapi import APIRouter, Depends, Request
from typing import Annotated, Literal

from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from uuid import uuid4
from os import getenv
from pathlib import Path

from .base_endpoints import load_base_endpoints
from sys import path

# appending a path
path.append('/app/common_libs')
from queue_interface import base_queue

# from ast import literal_eval

# app_name=getenv('app_name')
msg_topic = getenv('msg_topic')
msg_broker = base_queue(
    driver='redis', connection_setting={'redis_url': getenv('redis_url')}
)


endpoint_settings_path = Path(__file__).with_name('endpoint_settings.json').absolute()


router = APIRouter(prefix='/api/tasks', tags=['TASKS'])


def base_queue_endpoint(model, message_type: str):
    async def endpoint(
        request: Request, user: Annotated[model, Depends()]
    ):  # ,msg_type:str=message_type
        msg_type = message_type
        header = {'id': str(uuid4()), 'type': msg_type}
        message = dict(request.query_params)
        res = msg_broker.publish(msg_topic, message, header)
        return JSONResponse(content=jsonable_encoder({'response': res}))

    return endpoint


@router.get('/items/')
async def read_items(order_by: Literal['created_at', 'updated_at'] = 'created_at'):
    return {'order_by': order_by}


load_base_endpoints(router, base_queue_endpoint, endpoint_settings_path)
