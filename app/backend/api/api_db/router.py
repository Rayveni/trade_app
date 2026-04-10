from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from os import getenv
from sys import path

from .schemas import UserTables

# appending a path
path.append('/app/common_libs')
from utils import read_secrets
from db_interface import db_class

from pathlib import Path

task_db_creds_path = '/run/secrets/task_db_creds'
db_driver = db_class(
    driver='postgresql',
    connection_setting={
        **read_secrets(task_db_creds_path),
        **{'db_host': getenv('db_host')},
    },
)

router = APIRouter(prefix='/api/pg', tags=['POSTGRES'])
dir_path = Path(__file__).parent.resolve()
pg_conn = str(getenv('pg_url'))


def get_sql_query(query_name: str) -> str:
    _file_name = dir_path / f'{query_name}.sql'
    with open(_file_name, 'r') as f:
        _query = f.read()
    return _query


@router.get('/user_tables')
async def user_tables(return_type: UserTables = Depends()):
    return_type = return_type.return_type
    responce = db_driver.fetch_all(get_sql_query('user_tables'), return_type)
    return JSONResponse(content=jsonable_encoder(responce))


@router.get('/truncate/{table_name}')
async def truncate_table(table_name: str):
    res = db_driver.truncate(table_name)
    return JSONResponse(content=jsonable_encoder(res))
