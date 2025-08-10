from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from traceback import extract_tb
from sys import exc_info
#from app.pages.router import router as router_page
from backend.logger.logger import logger
from backend.logger.log_middleware import LogMiddleware
from backend.api.router import router as router_api
from backend.api.api_redis.router import router as router_redis
from backend.api.api_pg.router import router as router_pg
#from backend.scheduler.router import router as router_scheduler
app = FastAPI()

from backend.common_libs import *
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception:
        html_next_line = ' \r\n'
        ex_type, ex_value, ex_traceback = exc_info()
        trace_back = extract_tb(ex_traceback)

        stack_trace = [f'{html_next_line}Traceback:']
        for trace in trace_back:
            if not('site-packages/starlette/' in trace[0] or 'site-packages/fastapi/' in trace[0]):
                stack_trace.append(
                    f"File : {trace[0]} , Line :{trace[1]}, Func.Name : {trace[2]}, Message :{trace[3]}")
        stack_trace = html_next_line.join(stack_trace)
        return JSONResponse({"message": f"Error:{str(ex_value)}{stack_trace}"}, headers={"status": "error"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all sources
    allow_credentials=True,
    allow_methods=["*"],  # allow all methods
    allow_headers=["*"],  # allow all headers
)
app.middleware('http')(catch_exceptions_middleware)
app.add_middleware(LogMiddleware)
app.include_router(router_api)
app.include_router(router_redis)
app.include_router(router_pg)
#app.include_router(router_scheduler)
app.mount("/", StaticFiles(directory="frontend", html=True))
