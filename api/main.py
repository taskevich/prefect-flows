from typing import Any, Optional

from fastapi import FastAPI, Request
from pydantic import BaseModel


class RequestData(BaseModel):
    data: Optional[Any]


class ResponseData(BaseModel):
    data: Optional[Any]


app = FastAPI()


@app.get("/")
async def main():
    return {"error": False, "message": "OK", "payload": []}


@app.post("/data", response_model=ResponseData)
async def process_data(request: RequestData):
    return ResponseData(data=request.data)
