FROM python:3.11

WORKDIR /app

COPY ./shared ./shared
COPY webapp/. ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]