from fastapi import APIRouter, Form, File, UploadFile
# from loguru import logger
from fastapi.responses import StreamingResponse
from fastapi.requests import Request
import json
#from .schemas import UploadParams
from apscheduler.schedulers.background import BlockingScheduler

router = APIRouter(prefix="/scheduler", tags=["scheduler"])



@router.get("/start")
async def external_url(request: Request):
    return str(request.url)


