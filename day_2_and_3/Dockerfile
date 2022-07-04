FROM python:3.9.1
RUN pip install pandas sqlalchemy psycopg2
WORKDIR /app
COPY append_data_to_postgres_automated.py append_data_to_postgres_automated.py 
ENTRYPOINT [ "python","append_data_to_postgres_autmated.py "]