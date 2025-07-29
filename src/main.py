from fastapi import FastAPI

from api.lifespan import lifespan
from api.routers.distribution import router as distr_router
from api.routers.manage import router as manage_router

app = FastAPI(title="DNS-task", lifespan=lifespan)
app.include_router(manage_router)
app.include_router(distr_router)
