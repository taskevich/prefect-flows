import datetime
import os
import pandas as pd
import httpx

from prefect import flow, task, get_run_logger
from time import sleep
from prefect.task_runners import ThreadPoolTaskRunner
from pydantic import BaseModel, constr


class InputFlowData(BaseModel):
    inputFile: constr(strip_whitespace=True, min_length=1)
    chunkSize: int = 5000


@task
def request_api(row: dict):
    logger = get_run_logger()
    try:
        response = httpx.post(os.getenv("API_URL", "http://127.0.0.1:8000") + "/data", json=row)
        response.raise_for_status()
        return response.json()
    except Exception as ex:
        logger.error(f"Error occurred: {ex}")
        raise


@task
def load_data(input_csv: str, chunk_size: int):
    return pd.read_csv(input_csv, chunksize=chunk_size)


@task
def process_responses(responses: list):
    df = pd.DataFrame(responses)
    df.dropna(axis=1, how="all", inplace=True)
    return df


@task
def save_json(processed_data: pd.DataFrame):
    processed_data.to_json(
        f"/results/result-{datetime.datetime.now(tz=datetime.UTC).timestamp()}.json",
        orient="records"
    )


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=4))
def process_csv(request: InputFlowData):
    logger = get_run_logger()

    if not os.path.exists(request.inputFile):
        raise RuntimeError("No dataset file found.")

    logger.info("Загрузка данных из CSV")
    chunks = load_data(request.inputFile, request.chunkSize)
    responses = []

    for chunk in chunks:
        for _, row in chunk.iterrows():
            responses.append(request_api.submit(row.to_dict()))
        sleep(30)

    logger.info("Обработка ответов")
    processed_responses = process_responses(responses)

    logger.info(f"Сохранение результатов")
    save_json(processed_responses)


if __name__ == "__main__":
    process_csv(InputFlowData(
        inputFile="data.csv",
        chunkSize=5000
    ))
