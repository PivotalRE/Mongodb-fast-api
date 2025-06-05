FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Make scrapers visible to main.py inside api/
ENV PYTHONPATH=/app/api

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
