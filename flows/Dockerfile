FROM python:3.11-buster as builder

ENV TZ=Asia/Yekaterinburg
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 4200

ENV PREFECT_LOGGING_LEVEL=INFO
ENV WORKER_MEMORY_LIMIT=512Mi
ENV WORKER_CPU_LIMIT=500m

CMD ["sh", "-c", "prefect server start --host 0.0.0.0 & sleep 5 && python3 flows.py"]