FROM tiangolo/uvicorn-gunicorn-fastapi:python3.6

RUN pip install --upgrade pip
COPY app/requirements.txt app/requirements.txt
RUN pip install -r app/requirements.txt

#RUN sleep 10
COPY ./app /app