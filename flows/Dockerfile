FROM python:3.11-buster as builder

ENV TZ=Asia/Yekaterinburg
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED=1

COPY . .

RUN pip install -r requirements.txt

CMD ["sh", "-c", "prefect server start --host 0.0.0.0 & python3 flows.py"]