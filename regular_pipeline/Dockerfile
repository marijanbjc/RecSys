FROM python:3.11

WORKDIR /app

COPY ./shared ./shared
COPY regular_pipeline/. ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "main.py"]