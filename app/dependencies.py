from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession


async def get_db_session(request: Request):
    session_factory = request.app.state.db_session_factory
    async with session_factory() as session:
        yield session
