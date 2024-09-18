FROM python:3.11-buster as builder

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src .

CMD ["prefect", "run", "--name", "main"]
