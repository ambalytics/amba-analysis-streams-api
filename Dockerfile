FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

RUN pip install --upgrade pip
RUN pip install --upgrade sentry-sdk
COPY app/requirements.txt app/requirements.txt
RUN pip install -r app/requirements.txt

COPY ./app /app/app
COPY ./prestart.sh /app/
COPY ./scripts /app/scripts