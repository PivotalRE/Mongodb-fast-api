FROM python:3.11-slim

WORKDIR /app

COPY . .

# Set PYTHONPATH to include current working dir
ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "pvl.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
