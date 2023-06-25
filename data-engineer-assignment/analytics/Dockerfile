FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy pandas pymysql
WORKDIR /app
COPY . .
COPY utils /app/analytics/utils
CMD [ "python", "./analytics.py" ]