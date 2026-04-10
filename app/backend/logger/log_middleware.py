from starlette.middleware.base import BaseHTTPMiddleware
from .logger import logger


class LogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        logger.info(
            'Incoming request',
            extra={
                'request': {'method': request.method, 'url': str(request.url)},
                'response': {
                    'status_code': response.status_code,
                },
                'custom_fields': ['request', 'response'],
            },
        )
        return response
