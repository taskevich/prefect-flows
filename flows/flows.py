import datetime
import os
import pandas as pd
import httpx

from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from pydantic import BaseModel, constr

NUM_WORKERS = 4


class InputFlowData(BaseModel):
    inputFile: constr(strip_whitespace=True, min_length=1)
    chunkSize: int = 5000
    notifyId: int  # Предполагается мы знаем telegram id пользователя


@task
def request_api(row: dict):
    logger = get_run_logger()
    try:
        logger.info(f"Sending: {row}")
        response = httpx.post(os.getenv("API_URL", "http://127.0.0.1:8000") + "/data", json={"data": row})
        response.raise_for_status()
        return response.json()
    except Exception as ex:
        logger.error(f"Error occurred: {ex} on {row}")
        raise


@task
def load_data(input_csv: str, chunk_size: int):
    return pd.read_csv(input_csv, chunksize=chunk_size)


@task
def process_chunk(chunk: list[dict]):
    logger = get_run_logger()

    df = pd.DataFrame(chunk)
    df.dropna(axis=1, how="all", inplace=True)

    logger.info(f"Сохранение результатов")
    save_json(df)


@task
def save_json(processed_data: pd.DataFrame):
    processed_data.to_json(
        f"/results/result-{datetime.datetime.now(tz=datetime.UTC).timestamp()}.json",
        orient="records"
    )


@task
def send_to_telegram(notify_id: int):
    logger = get_run_logger()
    response = httpx.post(
        url=f"https://api.telegram.org/bot{os.getenv('BOT_TOKEN')}/sendmessage",
        json={"chat_id": notify_id, "text": "Обработка завершена"}
    )
    response.raise_for_status()
    logger.info("Уведомление в телеграмм отправлено")


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=NUM_WORKERS))
def process_csv(request: InputFlowData):
    logger = get_run_logger()

    if not os.path.exists(request.inputFile):
        raise RuntimeError("No dataset file found.")

    logger.info("Загрузка данных из CSV")
    chunks = load_data(request.inputFile, request.chunkSize)

    for chunk in chunks:
        responses = []

        for _, row in chunk.iterrows():
            responses.append(request_api(row.to_dict()))

        logger.info("Обработка ответов")

        chunk_size = len(responses) // NUM_WORKERS
        chunks = [responses[i:i + chunk_size] for i in range(0, len(responses), chunk_size)]

        for c in chunks:
            process_chunk.submit(c)

    logger.info(f"Отправка сообщения о завершении обработки {request.notifyId}")
    send_to_telegram(request.notifyId)


if __name__ == "__main__":
    process_csv(InputFlowData(
        inputFile="data.csv",
        chunkSize=300,
        notifyId=0
    ))
