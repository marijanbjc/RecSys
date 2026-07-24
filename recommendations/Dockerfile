FROM python:3.11

WORKDIR /app

COPY ./models.py ./
COPY ./watched_filter.py ./
COPY recommendations/. ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5001"]