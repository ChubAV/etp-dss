from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.domain.exceptions import (
    AVNotPassedError,
    FileNotFoundError,
    FileSizeExceededError,
    InvalidContentTypeError,
    TokenError,
)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(FileNotFoundError)
    async def _(request: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(TokenError)
    async def _(request: Request, exc: TokenError):
        return JSONResponse(status_code=401, content={"detail": str(exc)})

    @app.exception_handler(AVNotPassedError)
    async def _(request: Request, exc: AVNotPassedError):
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(FileSizeExceededError)
    async def _(request: Request, exc: FileSizeExceededError):
        return JSONResponse(status_code=413, content={"detail": str(exc)})

    @app.exception_handler(InvalidContentTypeError)
    async def _(request: Request, exc: InvalidContentTypeError):
        return JSONResponse(status_code=415, content={"detail": str(exc)})

    @app.exception_handler(ValueError)
    async def _(request: Request, exc: ValueError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})
