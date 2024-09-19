import os
from typing import Optional

import pandas as pd
import httpx

from prefect.tasks import task_input_hash
from prefect import flow, task, get_run_logger
from datetime import timedelta
from prefect.task_runners import ThreadPoolTaskRunner
from pydantic import BaseModel, constr


class InputFlowData(BaseModel):
    inputFile: Optional[constr(strip_whitespace=True, min_length=1)]
    outFile: Optional[constr(strip_whitespace=True, min_length=1)]
    chunkSize: Optional[int] = 5000


@task
def load_data(input_csv: str, chunk_size: int):
    return pd.read_csv(input_csv, chunksize=chunk_size, encoding="utf-8")


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(minutes=10), timeout_seconds=60)
def request_api(row: dict):
    logger = get_run_logger()
    try:
        # предполагается, что отправляем и получаем json
        response = httpx.post(os.getenv("API_URL", "http://127.0.0.1:8000") + "/data", json=row)
        response.raise_for_status()
        return response.json()
    except Exception as ex:
        logger.error(ex)
        raise


@task
def process_responses(responses: list):
    df = pd.DataFrame(responses)
    df.dropna(axis=1, how="all")
    return df


@task
def save_json(processed_data: pd.DataFrame, output_file: str):
    processed_data.to_json(output_file, orient="records")


@task
def send_to_telegram():
    pass


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=16))
def process_csv(request: InputFlowData):
    if not os.path.exists(request.inputFile):
        raise RuntimeError("No dataset file found.")

    logger = get_run_logger()
    chunks = load_data(request.inputFile, request.chunkSize)
    responses = []

    logger.info("Обработка чанков с данными")
    for chunk in chunks:
        for id, row in chunk.iterrows():
            responses.append(request_api.submit(row.to_dict()))

    logger.info(f"Обработка ответов API")
    processed_responses = process_responses(responses)

    logger.info(f"Сохранение результатов в {request.outFile}")
    save_json(processed_responses, request.outFile)


if __name__ == "__main__":
    if not os.path.exists("data.csv"):
        raise RuntimeError("No dataset file found.")
    process_csv(InputFlowData(
        inputFile="data.csv",
        outFile="result.json",
        chunkSize=5000
    ))
