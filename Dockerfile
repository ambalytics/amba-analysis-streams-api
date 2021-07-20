FROM node:latest AS build
COPY /amba-streams-frontend /vue
WORKDIR /vue
RUN npm install --save axios vue-axios
RUN npm install && npm run build
# this https://github.com/willfong/docker-fastapi-vue
# not https://developer.ibm.com/recipes/tutorials/a-best-practice-in-dockerizing-vue-js-application/

FROM tiangolo/uvicorn-gunicorn-fastapi:python3.6

RUN pip install --upgrade pip
COPY app/requirements.txt app/requirements.txt
RUN pip install -r app/requirements.txt

COPY --from=build /vue/dist /vue/dist
COPY ./app /app/app