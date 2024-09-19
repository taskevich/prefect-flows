from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
async def main():
    return {"error": False, "message": "OK", "payload": []}


@app.post("/data")
async def process_data(request: Request):
    return await request.json()
