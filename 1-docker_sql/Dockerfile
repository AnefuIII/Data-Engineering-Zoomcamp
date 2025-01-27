FROM python:3.9

WORKDIR /app

RUN pip install pandas

COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]