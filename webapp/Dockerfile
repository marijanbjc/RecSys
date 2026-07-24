FROM python:3.11

WORKDIR /app

COPY ./models.py ./
COPY ./watched_filter.py ./
COPY webapp/. ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]