from fastapi import APIRouter,Depends, Form, File, UploadFile
# from loguru import logger
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
#from .schemas import UploadParams
#from ..core.upload import create_export_file
from os import getenv,path
from .schemas import UserTables
from backend.common_libs.pg_wrapper import pg_wrapper
router = APIRouter(prefix="/api/pg", tags=["POSTGRES"])
dir_path = path.dirname(path.realpath(__file__))
pg_conn=str(getenv('pg_url'))

def get_sql_query(query_name:str)->str:
    _file_name=path.join(dir_path,f'{query_name}.sql')
    with open(_file_name,'r') as f: 
        _query = f.read() 
    return _query


@router.get("/url")
async def external_url(request: Request):
    return str(getenv('pg_url'))

@router.get("/user_tables")
async def user_tables(return_type: UserTables= Depends()):
    return_type=return_type.return_type
    success,responce=pg_wrapper(pg_conn).fetch_all(get_sql_query('user_tables'),return_type)
    if success:
        return JSONResponse(content=jsonable_encoder(responce))
    else:
        return responce

@router.get("/truncate/{table_name}")
async def truncate_table(table_name:str):
    res=pg_wrapper(pg_conn).truncate(table_name)
    return JSONResponse(content=jsonable_encoder(res))
    




