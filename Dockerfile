FROM mcr.microsoft.com/playwright/python:v1.51.0-noble

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install chromium

COPY . .

ENV PORT=8080
ENV DB_PATH=/data/mlcc_products.db
RUN mkdir -p /data

CMD ["python", "-c", "import os,uvicorn; uvicorn.run('app:app', host='0.0.0.0', port=int(os.environ.get('PORT','8080')))"]
