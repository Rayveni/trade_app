from fastapi import APIRouter, Form, File, UploadFile
# from loguru import logger
from fastapi.responses import StreamingResponse
from fastapi.requests import Request
import json
#from .schemas import UploadParams
#from ..core.upload import create_export_file

router = APIRouter(prefix="/api/redis", tags=["API","REDIS"])

@router.get("/url")
async def external_url(request: Request):
    return str(request.url)


