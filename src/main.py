from fastapi import FastAPI

from api.lifespan import lifespan
from api.routers.manage import router as manage_router

app = FastAPI(title="DNS-task", lifespan=lifespan)
app.include_router(manage_router)
